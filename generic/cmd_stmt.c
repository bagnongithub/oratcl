/*
 *  cmd_stmt.c --
 *
 *    Statement lifecycle and configuration (open/close/parse and stmt-level options).
 *
 *        - Applies fetch/prefetch and mode settings; maintains per-interp caches with no cross-interp sharing.
 *        - Safe in multi-threaded builds: statement objects guard ODPI handles and are refcounted per interp.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <inttypes.h>
#include <limits.h>
#include <string.h>

#include "cmd_int.h"
#include "dpi.h"

#ifndef DPI_DEFAULT_FETCH_ARRAY_SIZE
#define DPI_DEFAULT_FETCH_ARRAY_SIZE 100
#endif

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

int                      Oradpi_Cmd_Close(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int                      Oradpi_Cmd_Config(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int                      Oradpi_Cmd_Open(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int                      Oradpi_Cmd_Parse(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int                      Oradpi_Cmd_Stmt(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
static int               Oradpi_ConfigConn(Tcl_Interp *ip, OradpiConn *co, Tcl_Size objc, Tcl_Obj *const objv[]);
static int               Oradpi_ConfigStmt(Tcl_Interp *ip, OradpiStmt *s, Tcl_Size objc, Tcl_Obj *const objv[]);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/* ---- Connection config option table ---- */
static const char *const connOptNames[] = {"stmtcachesize", "fetcharraysize",  "prefetchrows",   "calltimeout",  "inlineLobs",       "foMaxAttempts",
                                           "foBackoffMs",   "foBackoffFactor", "foErrorClasses", "foDebounceMs", "failovercallback", NULL};
enum ConnOptIdx {
    COPT_STMTCACHE,
    COPT_FETCHARRAY,
    COPT_PREFETCHROWS,
    COPT_CALLTIMEOUT,
    COPT_INLINELOBS,
    COPT_FOMAXATT,
    COPT_FOBACKOFF,
    COPT_FOFACTOR,
    COPT_FOCLASSES,
    COPT_FODEBOUNCE,
    COPT_FOCALLBACK
};

/* Try Tcl_GetIndexFromObj, accepting an optional '-' prefix on the name.
 * Tries the obj directly first (for caching), then stripped of '-'. */
static int GetOptIndex(Tcl_Interp *ip, Tcl_Obj *nameObj, const char *const *table, const char *msg, int *idxOut) {
    if (Tcl_GetIndexFromObj(NULL, nameObj, table, msg, 0, idxOut) == TCL_OK)
        return TCL_OK;
    const char *s = Tcl_GetString(nameObj);
    if (s[0] == '-') {
        Tcl_Obj *stripped = Tcl_NewStringObj(s + 1, -1);
        Tcl_IncrRefCount(stripped);
        int rc = Tcl_GetIndexFromObj(ip, stripped, table, msg, 0, idxOut);
        Tcl_DecrRefCount(stripped);
        return rc;
    }
    /* Report error with full option list */
    return Tcl_GetIndexFromObj(ip, nameObj, table, msg, 0, idxOut);
}

static int SetStmtAndOwnerODPIError(Tcl_Interp *ip, OradpiStmt *s, const char *where) {
    dpiErrorInfo ei;
    if (!Oradpi_CaptureODPIError(&ei))
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, where ? where : "ODPI error");
    if (s && s->owner)
        Oradpi_SetErrorFromODPIInfo(NULL, (OradpiBase *)s->owner, where, &ei);
    return Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase *)s, where, &ei);
}

static int GetRequiredPositiveU32(Tcl_Interp *ip, Tcl_Obj *obj, uint32_t *out, const char *what) {
    uint32_t value = 0;
    if (Oradpi_GetUInt32FromObj(ip, obj, &value, what) != TCL_OK)
        return TCL_ERROR;
    if (value == 0) {
        Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s must be >= 1", what ? what : "value"));
        return TCL_ERROR;
    }
    *out = value;
    return TCL_OK;
}

/* ---- Statement config option table ---- */
static const char *const stmtOptNames[] = {"fetchrows", "prefetchrows", NULL};
enum StmtOptIdx { SOPT_FETCHROWS, SOPT_PREFETCHROWS };

int Oradpi_Cmd_Stmt(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    return Oradpi_Cmd_Open(cd, ip, objc, objv);
}

int Oradpi_Cmd_Config(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "handle ?name ?value??");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (s)
        return Oradpi_ConfigStmt(ip, s, objc, objv);
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (co)
        return Oradpi_ConfigConn(ip, co, objc, objv);
    return Oradpi_SetError(ip, NULL, -1, "invalid handle");
}

static int Oradpi_ConfigConn(Tcl_Interp *ip, OradpiConn *co, Tcl_Size objc, Tcl_Obj *const objv[]) {
    if (objc == 2) {
        uint32_t v   = 0;
        Tcl_Obj *res = Tcl_NewListObj(0, NULL);
        if (co->conn) {
            CONN_GATE_ENTER(co);
            if (dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
                co->stmtCacheSize = v;
            CONN_GATE_LEAVE(co);
        }
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("stmtcachesize", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(co->stmtCacheSize));

        uint32_t fas = co->fetchArraySize ? co->fetchArraySize : DPI_DEFAULT_FETCH_ARRAY_SIZE;
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("fetcharraysize", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(fas));

        LAPPEND_CHK(ip, res, Tcl_NewStringObj("prefetchrows", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(co->prefetchRows));

        if (co->conn) {
            CONN_GATE_ENTER(co);
            if (dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
                co->callTimeout = v;
            CONN_GATE_LEAVE(co);
        }
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("calltimeout", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(co->callTimeout));

        LAPPEND_CHK(ip, res, Tcl_NewStringObj("inlineLobs", -1));
        LAPPEND_CHK(ip, res, Tcl_NewBooleanObj(co->inlineLobs ? 1 : 0));

        LAPPEND_CHK(ip, res, Tcl_NewStringObj("foMaxAttempts", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(co->foMaxAttempts));
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("foBackoffMs", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(co->foBackoffMs));
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("foBackoffFactor", -1));
        LAPPEND_CHK(ip, res, Tcl_NewDoubleObj(co->foBackoffFactor));

        LAPPEND_CHK(ip, res, Tcl_NewStringObj("foErrorClasses", -1));
        Tcl_Obj *classes = Tcl_NewListObj(0, NULL);
        if (co->foErrorClasses & ORADPI_FO_CLASS_NETWORK)
            LAPPEND_CHK(ip, classes, Tcl_NewStringObj("network", -1));
        if (co->foErrorClasses & ORADPI_FO_CLASS_CONNLOST)
            LAPPEND_CHK(ip, classes, Tcl_NewStringObj("connlost", -1));
        LAPPEND_CHK(ip, res, classes);

        LAPPEND_CHK(ip, res, Tcl_NewStringObj("foDebounceMs", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(co->foDebounceMs));

        /* expose failovercallback in config listing */
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("failovercallback", -1));
        LAPPEND_CHK(ip, res, co->failoverCallback ? co->failoverCallback : Tcl_NewStringObj("", -1));

        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }

    if (objc == 3) {
        int idx;
        if (GetOptIndex(ip, objv[2], connOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum ConnOptIdx)idx) {
        case COPT_STMTCACHE: {
            uint32_t v = co->stmtCacheSize;
            if (co->conn) {
                CONN_GATE_ENTER(co);
                if (dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
                    co->stmtCacheSize = v;
                CONN_GATE_LEAVE(co);
            }
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(co->stmtCacheSize));
            return TCL_OK;
        }
        case COPT_FETCHARRAY: {
            uint32_t fas = co->fetchArraySize ? co->fetchArraySize : DPI_DEFAULT_FETCH_ARRAY_SIZE;
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(fas));
            return TCL_OK;
        }
        case COPT_PREFETCHROWS:
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(co->prefetchRows));
            return TCL_OK;
        case COPT_CALLTIMEOUT: {
            uint32_t v = co->callTimeout;
            if (co->conn) {
                CONN_GATE_ENTER(co);
                if (dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
                    co->callTimeout = v;
                CONN_GATE_LEAVE(co);
            }
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(co->callTimeout));
            return TCL_OK;
        }
        case COPT_INLINELOBS:
            Tcl_SetObjResult(ip, Tcl_NewBooleanObj(co->inlineLobs ? 1 : 0));
            return TCL_OK;
        case COPT_FOMAXATT:
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(co->foMaxAttempts));
            return TCL_OK;
        case COPT_FOBACKOFF:
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(co->foBackoffMs));
            return TCL_OK;
        case COPT_FOFACTOR:
            Tcl_SetObjResult(ip, Tcl_NewDoubleObj(co->foBackoffFactor));
            return TCL_OK;
        case COPT_FOCLASSES: {
            Tcl_Obj *classes = Tcl_NewListObj(0, NULL);
            if (co->foErrorClasses & ORADPI_FO_CLASS_NETWORK)
                LAPPEND_CHK(ip, classes, Tcl_NewStringObj("network", -1));
            if (co->foErrorClasses & ORADPI_FO_CLASS_CONNLOST)
                LAPPEND_CHK(ip, classes, Tcl_NewStringObj("connlost", -1));
            Tcl_SetObjResult(ip, classes);
            return TCL_OK;
        }
        case COPT_FODEBOUNCE:
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(co->foDebounceMs));
            return TCL_OK;
        case COPT_FOCALLBACK:
            Tcl_SetObjResult(ip, co->failoverCallback ? co->failoverCallback : Tcl_NewStringObj("", -1));
            return TCL_OK;
        }
        /* unreachable */
        return TCL_ERROR;
    }

    if ((objc % 2) != 0) {
        Tcl_WrongNumArgs(ip, 2, objv, "?-name value ...?");
        return TCL_ERROR;
    }
    for (Tcl_Size i = 2; i < objc; i += 2) {
        int idx;
        if (GetOptIndex(ip, objv[i], connOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum ConnOptIdx)idx) {
        case COPT_STMTCACHE: {
            if (Oradpi_GetUInt32FromObj(ip, objv[i + 1], &co->stmtCacheSize, "stmtcachesize") != TCL_OK)
                return TCL_ERROR;
            if (co->conn) {
                CONN_GATE_ENTER(co);
                if (dpiConn_setStmtCacheSize(co->conn, co->stmtCacheSize) != DPI_SUCCESS) {
                    CONN_GATE_LEAVE(co);
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_setStmtCacheSize");
                }
                CONN_GATE_LEAVE(co);
            }
            break;
        }
        case COPT_FETCHARRAY: {
            if (GetRequiredPositiveU32(ip, objv[i + 1], &co->fetchArraySize, "fetcharraysize") != TCL_OK)
                return TCL_ERROR;
            break;
        }
        case COPT_PREFETCHROWS: {
            if (Oradpi_GetUInt32FromObj(ip, objv[i + 1], &co->prefetchRows, "prefetchrows") != TCL_OK)
                return TCL_ERROR;
            break;
        }
        case COPT_CALLTIMEOUT: {
            if (Oradpi_GetUInt32FromObj(ip, objv[i + 1], &co->callTimeout, "calltimeout") != TCL_OK)
                return TCL_ERROR;
            if (co->conn) {
                CONN_GATE_ENTER(co);
                if (dpiConn_setCallTimeout(co->conn, co->callTimeout) != DPI_SUCCESS) {
                    CONN_GATE_LEAVE(co);
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_setCallTimeout");
                }
                CONN_GATE_LEAVE(co);
            }
            break;
        }
        case COPT_INLINELOBS: {
            int v = 0;
            if (Tcl_GetBooleanFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->inlineLobs = v ? 1 : 0;
            break;
        }
        case COPT_FOMAXATT: {
            uint32_t v = 0;
            if (Oradpi_GetUInt32FromObj(ip, objv[i + 1], &v, "foMaxAttempts") != TCL_OK)
                return TCL_ERROR;
            /* Enforce a reasonable upper bound;
             * foMaxAttempts = 4000000000 pins the executing thread in a retry
             * loop for an effectively unbounded time, destroying latency and
             * acting as a self-inflicted DoS via misconfiguration.
             * 1000 retries is already far beyond any realistic recovery scenario
             * and keeps the maximum total backoff under a few hours. */
#define ORADPI_FOMAXATTEMPTS_MAX 1000u
            if (v > ORADPI_FOMAXATTEMPTS_MAX) {
                Tcl_SetObjResult(ip, Tcl_ObjPrintf("foMaxAttempts value %" PRIu32 " exceeds maximum %u", v, ORADPI_FOMAXATTEMPTS_MAX));
                return TCL_ERROR;
            }
#undef ORADPI_FOMAXATTEMPTS_MAX
            co->foMaxAttempts = v;
            break;
        }
        case COPT_FOBACKOFF: {
            if (Oradpi_GetUInt32FromObj(ip, objv[i + 1], &co->foBackoffMs, "foBackoffMs") != TCL_OK)
                return TCL_ERROR;
            break;
        }
        case COPT_FOFACTOR: {
            double d = 0.0;
            if (Tcl_GetDoubleFromObj(ip, objv[i + 1], &d) != TCL_OK)
                return TCL_ERROR;
            co->foBackoffFactor = d;
            break;
        }
        case COPT_FOCLASSES: {
            Tcl_Size n = 0;
            if (Tcl_ListObjLength(ip, objv[i + 1], &n) != TCL_OK)
                return TCL_ERROR;
            uint32_t m = 0;
            for (Tcl_Size k = 0; k < n; k++) {
                Tcl_Obj *el = NULL;
                Tcl_ListObjIndex(ip, objv[i + 1], k, &el);
                Tcl_IncrRefCount(el);
                const char *t       = Tcl_GetString(el);
                int         unknown = 0;
                if (strcmp(t, "network") == 0)
                    m |= ORADPI_FO_CLASS_NETWORK;
                else if (strcmp(t, "connlost") == 0)
                    m |= ORADPI_FO_CLASS_CONNLOST;
                else
                    unknown = 1;
                if (unknown) {
                    Tcl_SetObjResult(ip, Tcl_ObjPrintf("unknown foErrorClasses value \"%s\"", t));
                    Tcl_DecrRefCount(el);
                    return TCL_ERROR;
                }
                Tcl_DecrRefCount(el);
            }
            co->foErrorClasses = m;
            break;
        }
        case COPT_FODEBOUNCE: {
            uint32_t v = 0;
            if (Oradpi_GetUInt32FromObj(ip, objv[i + 1], &v, "foDebounceMs") != TCL_OK)
                return TCL_ERROR;
            /* reject values that would overflow int at timer
             * scheduling time.  Tcl_CreateTimerHandler takes an int delay;
             * silently clamping to INT_MAX at use-time violates the principle
             * of rejecting rather than narrowing out-of-range config values. */
            if (v > (uint32_t)INT_MAX) {
                Tcl_SetObjResult(ip, Tcl_ObjPrintf("foDebounceMs value %" PRIu32 " exceeds maximum %d", v, INT_MAX));
                return TCL_ERROR;
            }
            co->foDebounceMs = v;
            break;
        }
        case COPT_FOCALLBACK: {
            /* Allow failovercallback to be set/changed via oraconfig,
             * matching the documented API.  Empty string clears the callback. */
            if (co->failoverCallback) {
                Tcl_DecrRefCount(co->failoverCallback);
                co->failoverCallback = NULL;
            }
            const char *cbStr = Tcl_GetString(objv[i + 1]);
            if (cbStr && cbStr[0] != '\0') {
                co->failoverCallback = objv[i + 1];
                Tcl_IncrRefCount(co->failoverCallback);
            }
            break;
        }
        }
    }
    /* After any config change, sync the behavioral policy snapshot
     * to the shared record so adopters inherit updated values. */
    Oradpi_SharedConnSyncBehavior(co);
    Tcl_Obj *tmpv[2] = {objv[0], objv[1]};
    return Oradpi_ConfigConn(ip, co, 2, tmpv);
}

static int Oradpi_ConfigStmt(Tcl_Interp *ip, OradpiStmt *s, Tcl_Size objc, Tcl_Obj *const objv[]) {
    if (objc == 2) {
        Tcl_Obj *res = Tcl_NewListObj(0, NULL);
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("fetchrows", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(s->fetchArray));
        uint32_t pr = s->prefetchRows ? s->prefetchRows : (s->owner ? s->owner->prefetchRows : 0);
        if (s->stmt) {
            CONN_GATE_ENTER(s->owner);
            (void)dpiStmt_getPrefetchRows(s->stmt, &pr);
            CONN_GATE_LEAVE(s->owner);
        }
        LAPPEND_CHK(ip, res, Tcl_NewStringObj("prefetchrows", -1));
        LAPPEND_CHK(ip, res, Oradpi_NewUInt32Obj(pr));
        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }
    if (objc == 3) {
        int idx;
        if (GetOptIndex(ip, objv[2], stmtOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum StmtOptIdx)idx) {
        case SOPT_FETCHROWS:
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(s->fetchArray));
            return TCL_OK;
        case SOPT_PREFETCHROWS: {
            uint32_t pr = s->prefetchRows ? s->prefetchRows : (s->owner ? s->owner->prefetchRows : 0);
            if (s->stmt) {
                CONN_GATE_ENTER(s->owner);
                (void)dpiStmt_getPrefetchRows(s->stmt, &pr);
                CONN_GATE_LEAVE(s->owner);
            }
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(pr));
            return TCL_OK;
        }
        }
        /* unreachable */
        return TCL_ERROR;
    }
    if (objc == 4) {
        int idx;
        if (GetOptIndex(ip, objv[2], stmtOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum StmtOptIdx)idx) {
        case SOPT_FETCHROWS: {
            if (GetRequiredPositiveU32(ip, objv[3], &s->fetchArray, "fetchrows") != TCL_OK)
                return TCL_ERROR;
            /* The output variable cache is sized to the old fetchArray.
             * Invalidate it so the next orafetch rebuilds vars with the new
             * maxArraySize; otherwise direct buffer access would overflow. */
            Oradpi_FreeFetchCache(s);
            if (s->stmt) {
                CONN_GATE_ENTER(s->owner);
                if (dpiStmt_setFetchArraySize(s->stmt, s->fetchArray) != DPI_SUCCESS) {
                    CONN_GATE_LEAVE(s->owner);
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_setFetchArraySize");
                }
                CONN_GATE_LEAVE(s->owner);
            }
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(s->fetchArray));
            return TCL_OK;
        }
        case SOPT_PREFETCHROWS: {
            uint32_t pr = 0;
            if (Oradpi_GetUInt32FromObj(ip, objv[3], &pr, "prefetchrows") != TCL_OK)
                return TCL_ERROR;
            s->prefetchRows = pr;
            if (s->stmt) {
                CONN_GATE_ENTER(s->owner);
                if (dpiStmt_setPrefetchRows(s->stmt, pr) != DPI_SUCCESS) {
                    CONN_GATE_LEAVE(s->owner);
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_setPrefetchRows");
                }
                CONN_GATE_LEAVE(s->owner);
            }
            /* Note: only affects this statement, not the connection default */
            Tcl_SetObjResult(ip, Oradpi_NewUInt32Obj(pr));
            return TCL_OK;
        }
        }
        /* unreachable */
        return TCL_ERROR;
    }
    Tcl_WrongNumArgs(ip, 1, objv, "handle ?name ?value??");
    return TCL_ERROR;
}

int Oradpi_Cmd_Open(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    /* Tcl 9 Tcl_Alloc panics on OOM; NewStmt cannot return NULL. */
    OradpiStmt *s = Oradpi_NewStmt(ip, co);
    Tcl_SetObjResult(ip, s->base.name);
    return TCL_OK;
}

int Oradpi_Cmd_Close(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s) {
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    }

    /* RemoveStmt cancels async, cleans bind stores, removes from hash, and frees */
    Oradpi_RemoveStmt(ip, s);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Parse(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    /* oraparse handle sql                (default: validates SQL with a PARSE_ONLY server round-trip)
     * oraparse handle -novalidate sql    (client-side prepare only; errors surface at execute time)
     *
     * The PARSE_ONLY round-trip catches syntax errors at parse time and is the
     * default.  Use -novalidate on hot paths where the SQL is known-good and
     * the per-parse round-trip cost is measurable. */
    if (objc < 3 || objc > 4) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-novalidate? sql-text");
        return TCL_ERROR;
    }
    int      doValidate = 1; /* default: validate */
    Tcl_Obj *sqlObj     = NULL;
    if (objc == 4) {
        const char *flag = Tcl_GetString(objv[2]);
        if (strcmp(flag, "-novalidate") != 0) {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-novalidate? sql-text");
            return TCL_ERROR;
        }
        doValidate = 0;
        sqlObj     = objv[3];
    } else {
        sqlObj = objv[2];
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "connection closed");
    Tcl_Size    sqlLen = 0;
    const char *sql    = Tcl_GetStringFromObj(sqlObj, &sqlLen);
    if (sqlLen < 0 || (uint64_t)sqlLen > UINT32_MAX)
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "SQL text exceeds maximum length");

    if (Oradpi_StmtWaitForAsync(s, 1, ORADPI_TEARDOWN_TIMEOUT_MS) == -3123)
        return Oradpi_SetError(ip, (OradpiBase *)s, -3123, "timed out waiting for async operation before re-parse");
    const char *stmtKey = Tcl_GetString(s->base.name);
    Oradpi_BindStoreForget(ip, stmtKey);
    Oradpi_PendingsForget(ip, stmtKey);

    CONN_GATE_ENTER(s->owner);
    if (s->stmt) {
        dpiStmt_close(s->stmt, NULL, 0);
        dpiStmt_release(s->stmt);
        s->stmt = NULL;
    }
    Oradpi_FreeFetchCache(s);
    s->stmtIsDML = s->stmtIsPLSQL = s->stmtIsQuery = 0;

    if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)sqlLen, NULL, 0, &s->stmt) != DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        return SetStmtAndOwnerODPIError(ip, s, "dpiConn_prepareStmt");
    }

    if (s->fetchArray) {
        if (dpiStmt_setFetchArraySize(s->stmt, s->fetchArray) != DPI_SUCCESS) {
            CONN_GATE_LEAVE(s->owner);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_setFetchArraySize");
        }
    }
    {
        uint32_t pr = s->prefetchRows ? s->prefetchRows : (s->owner ? s->owner->prefetchRows : 0);
        if (pr) {
            if (dpiStmt_setPrefetchRows(s->stmt, pr) != DPI_SUCCESS) {
                CONN_GATE_LEAVE(s->owner);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_setPrefetchRows");
            }
        }
    }
    /* When -novalidate is NOT given (the default), force a server round-trip
     * that catches syntax errors immediately.  Use -novalidate in hot paths
     * where the SQL is known-good and the per-parse round-trip is measurable. */
    if (doValidate) {
        uint32_t parseOnlyCols = 0;
        if (dpiStmt_execute(s->stmt, DPI_MODE_EXEC_PARSE_ONLY, &parseOnlyCols) != DPI_SUCCESS) {
            /* Clean up the server-rejected statement so subsequent oraexec
             * gets a clear "not prepared" error instead of retrying the
             * rejected SQL with a confusing duplicate error. */
            dpiStmt_close(s->stmt, NULL, 0);
            dpiStmt_release(s->stmt);
            s->stmt = NULL;
            CONN_GATE_LEAVE(s->owner);
            return SetStmtAndOwnerODPIError(ip, s, "dpiStmt_execute(PARSE_ONLY)");
        }
    }

    Oradpi_UpdateStmtType(s);
    CONN_GATE_LEAVE(s->owner);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
