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

#include <limits.h>
#include <string.h>

#include "cmd_int.h"
#include "dpi.h"

/* Failover error-class bitmask constants (must match cmd_exec.c) */
#define ORADPI_FO_CLASS_NETWORK 0x01
#define ORADPI_FO_CLASS_CONNLOST 0x02

extern dpiContext* Oradpi_GlobalDpiContext;

#ifndef DPI_DEFAULT_FETCH_ARRAY_SIZE
#define DPI_DEFAULT_FETCH_ARRAY_SIZE 100
#endif

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

int Oradpi_Cmd_Close(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Config(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Open(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Parse(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Stmt(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
static int Oradpi_ConfigConn(Tcl_Interp* ip, OradpiConn* co, Tcl_Size objc, Tcl_Obj* const objv[]);
static int Oradpi_ConfigStmt(Tcl_Interp* ip, OradpiStmt* s, Tcl_Size objc, Tcl_Obj* const objv[]);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/* ---- Connection config option table ---- */
static const char* const connOptNames[] = {"stmtcachesize",
                                           "fetcharraysize",
                                           "prefetchrows",
                                           "prefetchmemory",
                                           "calltimeout",
                                           "inlineLobs",
                                           "foMaxAttempts",
                                           "foBackoffMs",
                                           "foBackoffFactor",
                                           "foErrorClasses",
                                           "foDebounceMs",
                                           NULL};
enum ConnOptIdx
{
    COPT_STMTCACHE,
    COPT_FETCHARRAY,
    COPT_PREFETCHROWS,
    COPT_PREFETCHMEM,
    COPT_CALLTIMEOUT,
    COPT_INLINELOBS,
    COPT_FOMAXATT,
    COPT_FOBACKOFF,
    COPT_FOFACTOR,
    COPT_FOCLASSES,
    COPT_FODEBOUNCE
};

/* Try Tcl_GetIndexFromObj, accepting an optional '-' prefix on the name.
 * Tries the obj directly first (for caching), then stripped of '-'. */
static int GetOptIndex(Tcl_Interp* ip, Tcl_Obj* nameObj, const char* const* table, const char* msg, int* idxOut)
{
    if (Tcl_GetIndexFromObj(NULL, nameObj, table, msg, 0, idxOut) == TCL_OK)
        return TCL_OK;
    const char* s = Tcl_GetString(nameObj);
    if (s[0] == '-')
    {
        Tcl_Obj* stripped = Tcl_NewStringObj(s + 1, -1);
        Tcl_IncrRefCount(stripped);
        int rc = Tcl_GetIndexFromObj(ip, stripped, table, msg, 0, idxOut);
        Tcl_DecrRefCount(stripped);
        return rc;
    }
    /* Report error with full option list */
    return Tcl_GetIndexFromObj(ip, nameObj, table, msg, 0, idxOut);
}

/* ---- Statement config option table ---- */
static const char* const stmtOptNames[] = {"fetchrows", "prefetchrows", NULL};
enum StmtOptIdx
{
    SOPT_FETCHROWS,
    SOPT_PREFETCHROWS
};

int Oradpi_Cmd_Stmt(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    return Oradpi_Cmd_Open(cd, ip, objc, objv);
}

int Oradpi_Cmd_Config(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "handle ?name ?value??");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (s)
        return Oradpi_ConfigStmt(ip, s, objc, objv);
    OradpiConn* co = Oradpi_LookupConn(ip, objv[1]);
    if (co)
        return Oradpi_ConfigConn(ip, co, objc, objv);
    return Oradpi_SetError(ip, NULL, -1, "invalid handle");
}

static int Oradpi_ConfigConn(Tcl_Interp* ip, OradpiConn* co, Tcl_Size objc, Tcl_Obj* const objv[])
{
    if (objc == 2)
    {
        uint32_t v = 0;
        Tcl_Obj* res = Tcl_NewListObj(0, NULL);
        if (co->conn && dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
            co->stmtCacheSize = v;
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("stmtcachesize", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->stmtCacheSize));

        uint32_t fas = co->fetchArraySize ? co->fetchArraySize : DPI_DEFAULT_FETCH_ARRAY_SIZE;
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("fetcharraysize", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)fas));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("prefetchrows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->prefetchRows));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("prefetchmemory", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->prefetchMemory));

        if (co->conn && dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
            co->callTimeout = v;
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("calltimeout", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->callTimeout));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("inlineLobs", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewBooleanObj(co->inlineLobs ? 1 : 0));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foMaxAttempts", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->foMaxAttempts));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foBackoffMs", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->foBackoffMs));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foBackoffFactor", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewDoubleObj(co->foBackoffFactor));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foErrorClasses", -1));
        Tcl_Obj* classes = Tcl_NewListObj(0, NULL);
        if (co->foErrorClasses & ORADPI_FO_CLASS_NETWORK)
            Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("network", -1));
        if (co->foErrorClasses & ORADPI_FO_CLASS_CONNLOST)
            Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("connlost", -1));
        Tcl_ListObjAppendElement(ip, res, classes);

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foDebounceMs", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->foDebounceMs));

        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }

    if (objc == 3)
    {
        int idx;
        if (GetOptIndex(ip, objv[2], connOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum ConnOptIdx)idx)
        {
            case COPT_STMTCACHE:
            {
                uint32_t v = co->stmtCacheSize;
                if (co->conn && dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
                    co->stmtCacheSize = v;
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->stmtCacheSize));
                return TCL_OK;
            }
            case COPT_FETCHARRAY:
            {
                uint32_t fas = co->fetchArraySize ? co->fetchArraySize : DPI_DEFAULT_FETCH_ARRAY_SIZE;
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)fas));
                return TCL_OK;
            }
            case COPT_PREFETCHROWS:
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->prefetchRows));
                return TCL_OK;
            case COPT_PREFETCHMEM:
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->prefetchMemory));
                return TCL_OK;
            case COPT_CALLTIMEOUT:
            {
                uint32_t v = co->callTimeout;
                if (co->conn && dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
                    co->callTimeout = v;
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->callTimeout));
                return TCL_OK;
            }
            case COPT_INLINELOBS:
                Tcl_SetObjResult(ip, Tcl_NewBooleanObj(co->inlineLobs ? 1 : 0));
                return TCL_OK;
            case COPT_FOMAXATT:
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->foMaxAttempts));
                return TCL_OK;
            case COPT_FOBACKOFF:
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->foBackoffMs));
                return TCL_OK;
            case COPT_FOFACTOR:
                Tcl_SetObjResult(ip, Tcl_NewDoubleObj(co->foBackoffFactor));
                return TCL_OK;
            case COPT_FOCLASSES:
            {
                Tcl_Obj* classes = Tcl_NewListObj(0, NULL);
                if (co->foErrorClasses & ORADPI_FO_CLASS_NETWORK)
                    Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("network", -1));
                if (co->foErrorClasses & ORADPI_FO_CLASS_CONNLOST)
                    Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("connlost", -1));
                Tcl_SetObjResult(ip, classes);
                return TCL_OK;
            }
            case COPT_FODEBOUNCE:
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->foDebounceMs));
                return TCL_OK;
        }
        /* unreachable */
        return TCL_ERROR;
    }

    if ((objc % 2) != 0)
    {
        Tcl_WrongNumArgs(ip, 2, objv, "?-name value ...?");
        return TCL_ERROR;
    }
    for (Tcl_Size i = 2; i < objc; i += 2)
    {
        int idx;
        if (GetOptIndex(ip, objv[i], connOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum ConnOptIdx)idx)
        {
            case COPT_STMTCACHE:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                if (v < 0)
                    v = 0;
                if ((uint64_t)v > UINT32_MAX)
                    return Oradpi_SetError(ip, (OradpiBase*)co, -1, "stmtcachesize exceeds uint32 range");
                co->stmtCacheSize = (uint32_t)v;
                if (co->conn && dpiConn_setStmtCacheSize(co->conn, co->stmtCacheSize) != DPI_SUCCESS)
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiConn_setStmtCacheSize");
                break;
            }
            case COPT_FETCHARRAY:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->fetchArraySize = (uint32_t)(v > 0 && (uint64_t)v <= UINT32_MAX ? v : DPI_DEFAULT_FETCH_ARRAY_SIZE);
                break;
            }
            case COPT_PREFETCHROWS:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->prefetchRows = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                break;
            }
            case COPT_PREFETCHMEM:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->prefetchMemory = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                break;
            }
            case COPT_CALLTIMEOUT:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->callTimeout = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                if (co->conn && dpiConn_setCallTimeout(co->conn, co->callTimeout) != DPI_SUCCESS)
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiConn_setCallTimeout");
                break;
            }
            case COPT_INLINELOBS:
            {
                int v = 0;
                if (Tcl_GetBooleanFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->inlineLobs = v ? 1 : 0;
                break;
            }
            case COPT_FOMAXATT:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->foMaxAttempts = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                break;
            }
            case COPT_FOBACKOFF:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->foBackoffMs = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                break;
            }
            case COPT_FOFACTOR:
            {
                double d = 0.0;
                if (Tcl_GetDoubleFromObj(ip, objv[i + 1], &d) != TCL_OK)
                    return TCL_ERROR;
                co->foBackoffFactor = d;
                break;
            }
            case COPT_FOCLASSES:
            {
                Tcl_Obj** el = NULL;
                Tcl_Size n = 0;
                if (Tcl_ListObjGetElements(ip, objv[i + 1], &n, &el) != TCL_OK)
                    return TCL_ERROR;
                uint32_t m = 0;
                for (Tcl_Size k = 0; k < n; k++)
                {
                    const char* t = Tcl_GetString(el[k]);
                    if (strcmp(t, "network") == 0)
                        m |= ORADPI_FO_CLASS_NETWORK;
                    else if (strcmp(t, "connlost") == 0)
                        m |= ORADPI_FO_CLASS_CONNLOST;
                }
                co->foErrorClasses = m;
                break;
            }
            case COPT_FODEBOUNCE:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                    return TCL_ERROR;
                co->foDebounceMs = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                break;
            }
        }
    }
    Tcl_Obj* tmpv[2] = {objv[0], objv[1]};
    return Oradpi_ConfigConn(ip, co, 2, tmpv);
}

static int Oradpi_ConfigStmt(Tcl_Interp* ip, OradpiStmt* s, Tcl_Size objc, Tcl_Obj* const objv[])
{
    if (objc == 2)
    {
        Tcl_Obj* res = Tcl_NewListObj(0, NULL);
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("fetchrows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)s->fetchArray));
        uint32_t pr = s->owner ? s->owner->prefetchRows : 0;
        if (s->stmt)
            (void)dpiStmt_getPrefetchRows(s->stmt, &pr);
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("prefetchrows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)pr));
        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }
    if (objc == 3)
    {
        int idx;
        if (GetOptIndex(ip, objv[2], stmtOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum StmtOptIdx)idx)
        {
            case SOPT_FETCHROWS:
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)s->fetchArray));
                return TCL_OK;
            case SOPT_PREFETCHROWS:
            {
                uint32_t pr = s->owner ? s->owner->prefetchRows : 0;
                if (s->stmt)
                    (void)dpiStmt_getPrefetchRows(s->stmt, &pr);
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)pr));
                return TCL_OK;
            }
        }
        /* unreachable */
        return TCL_ERROR;
    }
    if (objc == 4)
    {
        int idx;
        if (GetOptIndex(ip, objv[2], stmtOptNames, "option", &idx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum StmtOptIdx)idx)
        {
            case SOPT_FETCHROWS:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[3], &v) != TCL_OK)
                    return TCL_ERROR;
                s->fetchArray = (uint32_t)(v > 0 && (uint64_t)v <= UINT32_MAX ? v : DPI_DEFAULT_FETCH_ARRAY_SIZE);
                if (s->stmt)
                {
                    if (dpiStmt_setFetchArraySize(s->stmt, s->fetchArray) != DPI_SUCCESS)
                        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_setFetchArraySize");
                }
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)s->fetchArray));
                return TCL_OK;
            }
            case SOPT_PREFETCHROWS:
            {
                Tcl_WideInt v = 0;
                if (Tcl_GetWideIntFromObj(ip, objv[3], &v) != TCL_OK)
                    return TCL_ERROR;
                uint32_t pr = (uint32_t)(v >= 0 && (uint64_t)v <= UINT32_MAX ? v : 0);
                if (s->stmt)
                {
                    if (dpiStmt_setPrefetchRows(s->stmt, pr) != DPI_SUCCESS)
                        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_setPrefetchRows");
                }
                /* Note: only affects this statement, not the connection default */
                Tcl_SetObjResult(ip, Tcl_NewIntObj((int)pr));
                return TCL_OK;
            }
        }
        /* unreachable */
        return TCL_ERROR;
    }
    Tcl_WrongNumArgs(ip, 1, objv, "handle ?name ?value??");
    return TCL_ERROR;
}

int Oradpi_Cmd_Open(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn* co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    OradpiStmt* s = Oradpi_NewStmt(ip, co);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "cannot allocate statement");
    Tcl_SetObjResult(ip, s->base.name);
    return TCL_OK;
}

int Oradpi_Cmd_Close(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle");
        return TCL_ERROR;
    }

    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
    {
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    }

    /* RemoveStmt cancels async, cleans bind stores, removes from hash, and frees */
    Oradpi_RemoveStmt(ip, s);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Parse(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle sql-text");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "connection closed");
    const char* sql = Tcl_GetString(objv[2]);

    (void)Oradpi_StmtWaitForAsync(s, 1, -1);
    const char* stmtKey = Tcl_GetString(s->base.name);
    Oradpi_BindStoreForget(ip, stmtKey);
    Oradpi_PendingsForget(ip, stmtKey);

    if (s->stmt)
    {
        dpiStmt_close(s->stmt, NULL, 0);
        dpiStmt_release(s->stmt);
        s->stmt = NULL;
    }

    Tcl_Size sqlLen = 0;
    (void)Tcl_GetStringFromObj(objv[2], &sqlLen);
    if (sqlLen < 0 || (uint64_t)sqlLen > UINT32_MAX)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "SQL text exceeds maximum length");
    if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)sqlLen, NULL, 0, &s->stmt) != DPI_SUCCESS)
    {
        dpiErrorInfo ei;
        dpiContext_getError(Oradpi_GlobalDpiContext, &ei);
        Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s, "dpiConn_prepareStmt", &ei);
        if (s->owner)
            Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s->owner, "dpiConn_prepareStmt", &ei);
        return TCL_ERROR;
    }
    s->executedInParse = 0;

    if (s->fetchArray)
    {
        if (dpiStmt_setFetchArraySize(s->stmt, s->fetchArray) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_setFetchArraySize");
    }
    if (s->owner && s->owner->prefetchRows)
    {
        if (dpiStmt_setPrefetchRows(s->stmt, s->owner->prefetchRows) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_setPrefetchRows");
    }
    Oradpi_UpdateStmtType(s);

    dpiStmtInfo info;
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS)
    {
        dpiErrorInfo ei;
        dpiContext_getError(Oradpi_GlobalDpiContext, &ei);
        Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s, "dpiStmt_getInfo", &ei);
        if (s->owner)
            Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s->owner, "dpiStmt_getInfo", &ei);
        return TCL_ERROR;
    }
    if (info.isQuery)
    {
        uint32_t bindCount = 0;
        if (dpiStmt_getBindCount(s->stmt, &bindCount) != DPI_SUCCESS)
        {
            dpiErrorInfo ei;
            dpiContext_getError(Oradpi_GlobalDpiContext, &ei);
            Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s, "dpiStmt_getBindCount", &ei);
            if (s->owner)
                Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s->owner, "dpiStmt_getBindCount", &ei);
            return TCL_ERROR;
        }
        if (bindCount == 0)
        {
            uint32_t numQueryCols = 0;
            if (dpiStmt_execute(s->stmt, DPI_MODE_EXEC_DEFAULT, &numQueryCols) != DPI_SUCCESS)
            {
                dpiErrorInfo ei;
                dpiContext_getError(Oradpi_GlobalDpiContext, &ei);
                Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s, "dpiStmt_execute", &ei);
                if (s->owner)
                    Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s->owner, "dpiStmt_execute", &ei);
                return TCL_ERROR;
            }
            s->executedInParse = 1;
        }
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
