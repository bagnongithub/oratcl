/*
 *  util.c --
 *
 *    Common utilities (error plumbing, handle naming, numeric parsing, and helpers used across commands).
 *
 *        - Thread-safe handle-name generator and lightweight parsing tuned for performance.
 *        - No interpreter-global effects; utilities operate on explicit state passed by callers.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <inttypes.h>
#include <limits.h>
#include <stdatomic.h>
#include <string.h>
#ifndef USE_TCL_STUBS
#define USE_TCL_STUBS
#endif
#include <tcl.h>

#include "cmd_int.h"
#include "dpi.h"

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static int              Oradpi_FailoverEventProc(Tcl_Event *evPtr, int flags);
static void             Oradpi_FailoverTimerProc(void *clientData);
Tcl_Obj                *Oradpi_NewHandleName(Tcl_Interp *ip, const char *prefix);
static void             Oradpi_PostFailoverEvent(OradpiConn *co, Tcl_Obj *message);
void                    Oradpi_RecordRows(OradpiBase *h, uint64_t rows);
int                     Oradpi_SetError(Tcl_Interp *ip, OradpiBase *h, int code, const char *msg);
int                     Oradpi_SetErrorFromODPI(Tcl_Interp *ip, OradpiBase *h, const char *where);
int                     Oradpi_SetErrorFromODPIInfo(Tcl_Interp *ip, OradpiBase *h, const char *where, const dpiErrorInfo *ei);
void                    Oradpi_UpdateStmtType(OradpiStmt *s);
static void             ReplaceObj(Tcl_Obj **slot, Tcl_Obj *val);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/* gHandleCounter: process-wide monotonic handle ID.  Uses a lock-free
 * relaxed atomic increment — uniqueness is the only requirement; strict
 * ordering relative to other memory operations is not needed. */
static _Atomic uint64_t gHandleCounter = 0;

Tcl_Obj                *Oradpi_NewHandleName(Tcl_Interp *ip, const char *prefix) {
    (void)ip;
    uint64_t id = atomic_fetch_add_explicit(&gHandleCounter, 1, memory_order_relaxed) + 1;
    return Tcl_ObjPrintf("%s%" PRIu64, prefix, id);
}

void Oradpi_RecordRows(OradpiBase *h, uint64_t rows) {
    if (h)
        h->msg.rows = rows;
}

static void ReplaceObj(Tcl_Obj **slot, Tcl_Obj *val) {
    if (val)
        Tcl_IncrRefCount(val);
    if (*slot)
        Tcl_DecrRefCount(*slot);
    *slot = val;
}

void Oradpi_UpdateStmtType(OradpiStmt *s) {
    if (!s || !s->stmt)
        return;
    dpiStmtInfo info;
    if (s->owner)
        CONN_GATE_ENTER(s->owner);
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
        if (s->owner)
            CONN_GATE_LEAVE(s->owner);
        return;
    }
    if (s->owner)
        CONN_GATE_LEAVE(s->owner);
    int t = 0;
    if (info.isQuery)
        t = 1;
    else if (info.isPLSQL)
        t = 2;
    else if (info.isDML)
        t = 3;
    else if (info.isDDL)
        t = 4;
    else if (info.isReturning)
        t = 5;
    s->base.msg.sqltype = t;
    s->stmtIsDML        = info.isDML ? 1 : 0;
    s->stmtIsPLSQL      = info.isPLSQL ? 1 : 0;
    s->stmtIsQuery      = info.isQuery ? 1 : 0;
}

typedef struct OradpiFailoverEvent {
    Tcl_Event   header;
    Tcl_Interp *ip;      /* owning interp */
    Tcl_Obj    *ldaName; /* connection handle name */
    Tcl_Obj    *message; /* error text */
} OradpiFailoverEvent;

static void Oradpi_FailoverTimerProc(void *clientData) {
    OradpiConn *co = (OradpiConn *)clientData;
    if (!co)
        return;
    co->foTimer          = NULL;
    co->foTimerScheduled = 0;

    if (!co->ownerIp || !co->failoverCallback) {
        if (co->foPendingMsg) {
            Tcl_DecrRefCount(co->foPendingMsg);
            co->foPendingMsg = NULL;
        }
        return;
    }
    if (Tcl_InterpDeleted(co->ownerIp)) {
        if (co->foPendingMsg) {
            Tcl_DecrRefCount(co->foPendingMsg);
            co->foPendingMsg = NULL;
        }
        return;
    }

    /* cmd is freshly duplicated (refcount 1, unshared) — Tcl_ListObjAppendElement
     * cannot fail on an unshared list, but we check for defensive correctness. */
    Tcl_Obj *cmd = Tcl_DuplicateObj(co->failoverCallback);
    Tcl_IncrRefCount(cmd);

    /* snapshot foPendingMsg into a local and detach from co BEFORE
     * eval, because the callback can legally call oralogoff and free co.
     * transfer ownership instead of leaking — decrement the
     * connection-held ref after acquiring the local ref. */
    Tcl_Obj *pendingMsg = co->foPendingMsg;
    if (pendingMsg)
        Tcl_IncrRefCount(pendingMsg);
    if (co->foPendingMsg)
        Tcl_DecrRefCount(co->foPendingMsg);
    co->foPendingMsg     = NULL;

    Tcl_Interp *evalIp   = co->ownerIp;

    /* use consistent checked-append pattern */
    int         appendOk = 1;
    if (Tcl_ListObjAppendElement(evalIp, cmd, co->base.name) != TCL_OK || Tcl_ListObjAppendElement(evalIp, cmd, Tcl_NewStringObj("recoverable", -1)) != TCL_OK ||
        Tcl_ListObjAppendElement(evalIp, cmd, pendingMsg ? pendingMsg : Tcl_NewStringObj("", -1)) != TCL_OK)
        appendOk = 0;

    if (!appendOk) {
        Tcl_DecrRefCount(cmd);
        if (pendingMsg)
            Tcl_DecrRefCount(pendingMsg);
        return;
    }

    /* Report callback errors via Tcl_BackgroundException
     * instead of silently discarding them. */
    int evalRc = Tcl_EvalObjEx(evalIp, cmd, TCL_EVAL_GLOBAL);
    if (evalRc != TCL_OK)
        Tcl_BackgroundException(evalIp, evalRc);
    Tcl_DecrRefCount(cmd);

    /* only touch locals after eval, never dereference co */
    if (pendingMsg)
        Tcl_DecrRefCount(pendingMsg);
}

static int Oradpi_FailoverEventProc(Tcl_Event *evPtr, int flags) {
    (void)flags;
    OradpiFailoverEvent *fe = (OradpiFailoverEvent *)evPtr;

    if (!fe->ip || Tcl_InterpDeleted(fe->ip)) {
        if (fe->ip)
            Tcl_Release(fe->ip);
        if (fe->ldaName)
            Tcl_DecrRefCount(fe->ldaName);
        if (fe->message)
            Tcl_DecrRefCount(fe->message);
        /* free the event struct on every return path.
         * Tcl's event model requires the handler to free its own event. */
        Tcl_Free((char *)fe);
        return 1;
    }

    /* Resolve connection in the owning interp */
    OradpiConn *co = Oradpi_LookupConn(fe->ip, fe->ldaName);
    if (!co) {
        Tcl_Release(fe->ip);
        Tcl_DecrRefCount(fe->ldaName);
        Tcl_DecrRefCount(fe->message);
        Tcl_Free((char *)fe);
        return 1;
    }

    /* Update pending message (replace any previous) */
    if (co->foPendingMsg)
        Tcl_DecrRefCount(co->foPendingMsg);
    co->foPendingMsg = fe->message;
    Tcl_IncrRefCount(co->foPendingMsg);

    /* Start debounce timer if not already scheduled */
    if (!co->foTimerScheduled) {
        /* foDebounceMs is validated <= INT_MAX at oraconfig time,
         * so the cast is safe; keep the default 250 ms if unset. */
        uint32_t ms          = co->foDebounceMs ? co->foDebounceMs : 250;
        int      delay       = (int)ms;
        co->foTimer          = Tcl_CreateTimerHandler(delay, Oradpi_FailoverTimerProc, co);
        co->foTimerScheduled = 1;
    }

    Tcl_Release(fe->ip);
    Tcl_DecrRefCount(fe->ldaName);
    Tcl_DecrRefCount(fe->message);
    Tcl_Free((char *)fe);
    return 1;
}

static void Oradpi_PostFailoverEvent(OradpiConn *co, Tcl_Obj *message) {
    Tcl_Obj *msgObj = message ? message : Tcl_NewStringObj("recoverable error", -1);
    Tcl_IncrRefCount(msgObj);

    if (!co || !co->ownerIp || !co->ownerTid) {
        Tcl_DecrRefCount(msgObj);
        return;
    }

    OradpiFailoverEvent *fe = (OradpiFailoverEvent *)Tcl_Alloc(sizeof(*fe));
    memset(fe, 0, sizeof(*fe));
    fe->header.proc = Oradpi_FailoverEventProc;
    fe->ip          = co->ownerIp;
    Tcl_Preserve(fe->ip);
    fe->ldaName = co->base.name;
    Tcl_IncrRefCount(fe->ldaName);
    fe->message = msgObj;
    Tcl_ThreadQueueEvent(co->ownerTid, &fe->header, TCL_QUEUE_TAIL);
    Tcl_ThreadAlert(co->ownerTid);
}

int Oradpi_SetErrorFromODPIInfo(Tcl_Interp *ip, OradpiBase *h, const char *where, const dpiErrorInfo *ei) {
    if (h) {
        h->msg.rc          = (int)ei->code;
        h->msg.ocicode     = (int)ei->code;
        h->msg.recoverable = (ei->isRecoverable ? 1 : 0);
        h->msg.warning     = (ei->isWarning ? 1 : 0);
        h->msg.offset      = ei->offset;
        h->msg.peo         = ei->offset;
        /* Map ei->fnName to msg.fn (ODPI function that triggered the error) */
        ReplaceObj(&h->msg.fn, Tcl_NewStringObj(ei->fnName ? ei->fnName : (where ? where : "ODPI"), -1));
        ReplaceObj(&h->msg.sqlstate, Tcl_NewStringObj(ei->sqlState ? ei->sqlState : "", -1));
        ReplaceObj(&h->msg.action, Tcl_NewStringObj(ei->action ? ei->action : "", -1));
        ReplaceObj(&h->msg.error, Tcl_NewStringObj(ei->message ? ei->message : "ODPI error", -1));
    }

    if (h && ei->isRecoverable) {
        OradpiConn *co    = NULL;
        const char *hname = h->name ? Tcl_GetString(h->name) : NULL;
        if (hname && strncmp(hname, "oraL", 4) == 0) {
            co = (OradpiConn *)h;
        } else if (hname && strncmp(hname, "oraS", 4) == 0) {
            OradpiStmt *s = (OradpiStmt *)(void *)h;
            co            = s->owner;
        }
        /* Only post failover events from the connection's owner
         * thread.  PostFailoverEvent touches co->base.name, co->ownerIp,
         * and co->failoverCallback with Tcl_IncrRefCount — these are
         * unsynchronized Tcl_Obj operations that must not race with the
         * interpreter thread modifying the same fields via oraconfig or
         * oralogoff.  Calls from async worker threads are safe to skip
         * here because the async completion path (orawaitasync) will
         * report the error on the interpreter thread. */
        if (co && co->failoverCallback && co->ownerTid == Tcl_GetCurrentThread()) {
            Tcl_Obj *msg = Tcl_NewStringObj(ei->message ? ei->message : "recoverable error", -1);
            Oradpi_PostFailoverEvent(co, msg);
        }
    }

    if (ip) {
        Tcl_Obj *res = Tcl_NewStringObj(ei->message ? ei->message : "ODPI error", -1);
        Tcl_SetObjResult(ip, res);
        Tcl_SetErrorCode(ip, "ORATCL", "ODPI", where ? where : "ODPI", NULL);
    }
    return TCL_ERROR;
}

int Oradpi_SetErrorFromODPI(Tcl_Interp *ip, OradpiBase *h, const char *where) {
    dpiErrorInfo ei;
    if (!Oradpi_CaptureODPIError(&ei))
        return Oradpi_SetError(ip, h, -1, where ? where : "ODPI error");
    return Oradpi_SetErrorFromODPIInfo(ip, h, where, &ei);
}

int Oradpi_SetError(Tcl_Interp *ip, OradpiBase *h, int code, const char *msg) {
    if (h) {
        h->msg.rc          = code;
        h->msg.ocicode     = code;
        h->msg.recoverable = 0;
        ReplaceObj(&h->msg.fn, Tcl_NewStringObj("Oratcl", -1));
        ReplaceObj(&h->msg.error, Tcl_NewStringObj(msg ? msg : "error", -1));
    }
    if (ip) {
        Tcl_SetObjResult(ip, Tcl_NewStringObj(msg ? msg : "error", -1));
        Tcl_SetErrorCode(ip, "ORATCL", "CLIENT", NULL);
    }
    return TCL_ERROR;
}
