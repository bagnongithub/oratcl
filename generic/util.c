/*
 *  util.c --
 *
 *    Common utilities (error plumbing, handle naming, numeric parsing, and helpers used across commands).
 *
 *        - Thread‑safe handle‑name generator and lightweight parsing tuned for performance.
 *        - No interpreter‑global effects; utilities operate on explicit state passed by callers.
 *
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <string.h>
#include <tcl.h>

#include "cmd_int.h"
#include "dpi.h"
#include "state.h"

/* ------------------------------------------------------------------------- *
 * Internal state
 * ------------------------------------------------------------------------- */

static Tcl_Mutex   gHandleMutex;

extern dpiContext *Oradpi_GlobalDpiContext;

Tcl_Obj           *Oradpi_NewHandleName(Tcl_Interp *ip, const char *prefix) {
    static int counter = 0;

    Tcl_MutexLock(&gHandleMutex);
    int id = ++counter;
    Tcl_MutexUnlock(&gHandleMutex);
    Tcl_Obj *name = Tcl_NewStringObj(prefix, -1);
    Tcl_AppendToObj(name, Tcl_GetString(Tcl_NewLongObj((Tcl_WideInt)id)), -1);
    return name;
}

int Oradpi_IsNumberObj(Tcl_Obj *o, long long *asInt, double *asDbl, int *isInt) {
    if (Tcl_GetWideIntFromObj(NULL, o, asInt) == TCL_OK) {
        if (isInt)
            *isInt = 1;
        return 1;
    }
    if (Tcl_GetDoubleFromObj(NULL, o, asDbl) == TCL_OK) {
        if (isInt)
            *isInt = 0;
        return 1;
    }
    return 0;
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
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS)
        return;
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
}

typedef struct OradpiFailoverEvent {
    Tcl_Event   header;
    Tcl_Interp *ip;      /* owning interp */
    Tcl_Obj    *ldaName; /* connection handle name */
    Tcl_Obj    *message; /* error text */
} OradpiFailoverEvent;

static void Oradpi_FailoverTimerProc(void *clientData) {
    OradpiConn *co       = (OradpiConn *)clientData;
    co->foTimer          = NULL;
    co->foTimerScheduled = 0;

    if (!co || !co->ownerIp || !co->failoverCallback) {
        if (co && co->foPendingMsg) {
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

    Tcl_Obj *cmd = Tcl_DuplicateObj(co->failoverCallback);
    Tcl_IncrRefCount(cmd);
    Tcl_ListObjAppendElement(co->ownerIp, cmd, co->base.name);
    Tcl_ListObjAppendElement(co->ownerIp, cmd, Tcl_NewStringObj("recoverable", -1));
    Tcl_ListObjAppendElement(co->ownerIp, cmd, co->foPendingMsg ? co->foPendingMsg : Tcl_NewStringObj("", -1));

    (void)Tcl_EvalObjEx(co->ownerIp, cmd, TCL_EVAL_GLOBAL);
    Tcl_DecrRefCount(cmd);

    if (co->foPendingMsg) {
        Tcl_DecrRefCount(co->foPendingMsg);
        co->foPendingMsg = NULL;
    }
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
        return 1;
    }

    /* Resolve connection in the owning interp */
    OradpiConn *co = Oradpi_LookupConn(fe->ip, fe->ldaName);
    if (!co) {
        Tcl_Release(fe->ip);
        Tcl_DecrRefCount(fe->ldaName);
        Tcl_DecrRefCount(fe->message);
        return 1;
    }

    /* Update pending message (replace any previous) */
    if (co->foPendingMsg)
        Tcl_DecrRefCount(co->foPendingMsg);
    co->foPendingMsg = fe->message;
    Tcl_IncrRefCount(co->foPendingMsg);

    /* Start debounce timer if not already scheduled */
    if (!co->foTimerScheduled) {
        int delay            = (int)(co->foDebounceMs ? co->foDebounceMs : 250);
        co->foTimer          = Tcl_CreateTimerHandler(delay, Oradpi_FailoverTimerProc, co);
        co->foTimerScheduled = 1;
    }

    Tcl_Release(fe->ip);
    Tcl_DecrRefCount(fe->ldaName);
    Tcl_DecrRefCount(fe->message);
    return 1;
}

static void Oradpi_PostFailoverEvent(OradpiConn *co, Tcl_Obj *message) {
    if (!co || !co->ownerIp || !co->ownerTid)
        return;

    OradpiFailoverEvent *fe = (OradpiFailoverEvent *)Tcl_Alloc(sizeof(*fe));
    memset(fe, 0, sizeof(*fe));
    fe->header.proc = Oradpi_FailoverEventProc;
    fe->ip          = co->ownerIp;
    Tcl_Preserve(fe->ip);
    fe->ldaName = co->base.name;
    Tcl_IncrRefCount(fe->ldaName);
    fe->message = message ? message : Tcl_NewStringObj("recoverable error", -1);
    Tcl_IncrRefCount(fe->message);
    Tcl_ThreadQueueEvent(co->ownerTid, &fe->header, TCL_QUEUE_TAIL);
    Tcl_ThreadAlert(co->ownerTid);
}

int Oradpi_SetErrorFromODPI(Tcl_Interp *ip, OradpiBase *h, const char *where) {
    dpiErrorInfo ei;
    dpiContext_getError(Oradpi_GlobalDpiContext, &ei);

    if (h) {
        h->msg.rc          = (int)ei.code;
        h->msg.ocicode     = (int)ei.code;
        h->msg.recoverable = (ei.isRecoverable ? 1 : 0);
        h->msg.offset      = ei.offset;
        ReplaceObj(&h->msg.fn, Tcl_NewStringObj(where ? where : "ODPI", -1));
        ReplaceObj(&h->msg.sqlstate, Tcl_NewStringObj(ei.sqlState ? ei.sqlState : "", -1));
        ReplaceObj(&h->msg.action, NULL);
        ReplaceObj(&h->msg.error, Tcl_NewStringObj(ei.message ? ei.message : "ODPI error", -1));
    }

    if (h && ei.isRecoverable) {
        OradpiConn *co    = NULL;
        const char *hname = h->name ? Tcl_GetString(h->name) : NULL;
        if (hname && strncmp(hname, "oraL", 4) == 0) {
            co = (OradpiConn *)h;
        } else if (hname && strncmp(hname, "oraS", 4) == 0) {
            OradpiStmt *s = (OradpiStmt *)(void *)h;
            co            = s->owner;
        }
        if (co && co->failoverCallback) {
            Tcl_Obj *msg = Tcl_NewStringObj(ei.message ? ei.message : "recoverable error", -1);
            Oradpi_PostFailoverEvent(co, msg);
            Tcl_DecrRefCount(msg);
        }
    }

    if (ip) {
        Tcl_Obj *res = Tcl_NewStringObj(ei.message ? ei.message : "ODPI error", -1);
        Tcl_SetObjResult(ip, res);
        Tcl_SetErrorCode(ip, "ORATCL", "ODPI", where ? where : "ODPI", NULL);
    }
    return TCL_ERROR;
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
