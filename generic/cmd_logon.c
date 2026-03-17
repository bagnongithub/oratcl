/*
 *  cmd_logon.c --
 *
 *    Connection management commands and helpers (logon/logoff, pooled and dedicated connections).
 *
 *        - Parses connect strings and external auth; opens dpiConn via ODPI-C only.
 *        - Registers connections in the current interpreter while avoiding duplicate client loads across interps.
 *        - Thread-aware: minimal shared mutable state; process-wide pieces guarded by Tcl mutexes.
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

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

int Oradpi_Cmd_Autocommit(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Break(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Info(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Logoff(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Logon(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
static void Oradpi_ParseConnect(const char* cs,
                                const char** user,
                                uint32_t* ulen,
                                const char** pw,
                                uint32_t* plen,
                                const char** db,
                                uint32_t* dblen,
                                int* extAuth);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/* m-2: Parse connect string with support for double-quoted passwords.
 * Format: user/"pass@word"@db  or  user/password@db  or  /  (ext auth)
 * Passwords containing @ or / must be double-quoted.
 * V-7 fix: all length computations use size_t with range checks before
 * narrowing to uint32_t, preventing truncation on pathological inputs. */
static void Oradpi_ParseConnect(const char* cs,
                                const char** user,
                                uint32_t* ulen,
                                const char** pw,
                                uint32_t* plen,
                                const char** db,
                                uint32_t* dblen,
                                int* extAuth)
{
    *user = *pw = *db = NULL;
    *ulen = *plen = *dblen = 0;
    *extAuth = 0;
    if (!cs)
        return;
    const char* slash = strchr(cs, '/');

    /* Helper macro: safely narrow a size_t to uint32_t, clamping at UINT32_MAX */
#define SAFE_U32(sz) ((sz) > UINT32_MAX ? (uint32_t)UINT32_MAX : (uint32_t)(sz))

    /* External auth: starts with / */
    if (cs[0] == '/' && (!slash || slash == cs))
    {
        *extAuth = 1;
        const char* at = strchr(cs, '@');
        if (at && at[1])
        {
            *db = at + 1;
            *dblen = SAFE_U32(strlen(*db));
        }
        return;
    }

    /* Look for quoted password: user/"..."@db */
    if (slash && slash[1] == '"')
    {
        *user = cs;
        *ulen = SAFE_U32((size_t)(slash - cs));
        const char* pwStart = slash + 2; /* skip /" */
        const char* closeQuote = strchr(pwStart, '"');
        if (closeQuote)
        {
            *pw = pwStart;
            *plen = SAFE_U32((size_t)(closeQuote - pwStart));
            /* After closing quote, expect @ or end of string */
            if (closeQuote[1] == '@' && closeQuote[2])
            {
                *db = closeQuote + 2;
                *dblen = SAFE_U32(strlen(*db));
            }
        }
        else
        {
            /* No closing quote — treat entire remainder as password */
            *pw = pwStart;
            *plen = SAFE_U32(strlen(pwStart));
        }
        return;
    }

    /* Unquoted: original logic using first @ and first / */
    const char* at = strchr(cs, '@');
    if (at)
    {
        if (slash && slash < at)
        {
            *user = cs;
            *ulen = SAFE_U32((size_t)(slash - cs));
            *pw = slash + 1;
            *plen = SAFE_U32((size_t)(at - slash - 1));
        }
        else
        {
            *user = cs;
            *ulen = SAFE_U32((size_t)(at - cs));
        }
        *db = at + 1;
        *dblen = SAFE_U32(strlen(*db));
    }
    else
    {
        if (slash)
        {
            *user = cs;
            *ulen = SAFE_U32((size_t)(slash - cs));
            *pw = slash + 1;
            *plen = SAFE_U32(strlen(*pw));
        }
        else
        {
            *user = cs;
            *ulen = SAFE_U32(strlen(cs));
        }
    }
#undef SAFE_U32
}

/*
 * oralogon connect-str ?-pool {min max incr}? ?-homogeneous bool?
 *         ?-getmode wait|nowait|forceget|timedwait? ?-failovercallback proc?
 *
 *   Opens a dedicated or pooled Oracle connection. connect-str may be
 *   user/password@db, user/"quoted@pass"@db, or / (external auth).
 *   Returns: connection handle name (e.g., "oraL1") on success.
 *   Errors:  ODPI-C connect/pool errors; invalid option values.
 *   Thread-safety: safe — allocates per-interp state only.
 */
int Oradpi_Cmd_Logon(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2)
    {
        Tcl_WrongNumArgs(ip,
                         1,
                         objv,
                         "connect-str ?-pool min max incr? ?-homogeneous bool? "
                         "?-getmode wait|nowait|forceget|timedwait? "
                         "?-failovercallback proc?");
        return TCL_ERROR;
    }
    const char* connstr = Tcl_GetString(objv[1]);
    int usePool = 0, homogeneous = 1;
    Tcl_WideInt minS = 1, maxS = 4, incS = 1;
    int getmode = DPI_MODE_POOL_GET_WAIT;
    Tcl_Obj* failoverCb = NULL;

    static const char* const logonOpts[] = {"-pool", "-homogeneous", "-getmode", "-failovercallback", NULL};
    enum LogonOptIdx
    {
        LOPT_POOL,
        LOPT_HOMOGENEOUS,
        LOPT_GETMODE,
        LOPT_FAILOVERCB
    };

    static const char* const getmodeNames[] = {"wait", "nowait", "forceget", "timedwait", NULL};
    static const int getmodeValues[] = {
        DPI_MODE_POOL_GET_WAIT, DPI_MODE_POOL_GET_NOWAIT, DPI_MODE_POOL_GET_FORCEGET, DPI_MODE_POOL_GET_TIMEDWAIT};

    for (Tcl_Size i = 2; i < objc; i++)
    {
        int optIdx;
        if (Tcl_GetIndexFromObj(ip, objv[i], logonOpts, "option", 0, &optIdx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum LogonOptIdx)optIdx)
        {
            case LOPT_POOL:
            {
                if (i + 1 >= objc)
                    return Oradpi_SetError(ip, NULL, -1, "-pool requires arguments");
                Tcl_Size n = 0;
                Tcl_Obj** elems = NULL;
                if (Tcl_ListObjGetElements(ip, objv[i + 1], &n, &elems) == TCL_OK && n == 3)
                {
                    i++;
                    if (Tcl_GetWideIntFromObj(ip, elems[0], &minS) != TCL_OK)
                        return TCL_ERROR;
                    if (Tcl_GetWideIntFromObj(ip, elems[1], &maxS) != TCL_OK)
                        return TCL_ERROR;
                    if (Tcl_GetWideIntFromObj(ip, elems[2], &incS) != TCL_OK)
                        return TCL_ERROR;
                }
                else if (i + 3 < objc)
                {
                    if (Tcl_GetWideIntFromObj(ip, objv[++i], &minS) != TCL_OK)
                        return TCL_ERROR;
                    if (Tcl_GetWideIntFromObj(ip, objv[++i], &maxS) != TCL_OK)
                        return TCL_ERROR;
                    if (Tcl_GetWideIntFromObj(ip, objv[++i], &incS) != TCL_OK)
                        return TCL_ERROR;
                }
                else
                    return Oradpi_SetError(ip, NULL, -1, "-pool requires {min max incr} or min max incr");
                usePool = 1;
                break;
            }
            case LOPT_HOMOGENEOUS:
                if (i + 1 >= objc)
                    return Oradpi_SetError(ip, NULL, -1, "-homogeneous requires a boolean");
                if (Tcl_GetBooleanFromObj(ip, objv[++i], &homogeneous) != TCL_OK)
                    return TCL_ERROR;
                break;
            case LOPT_GETMODE:
            {
                if (i + 1 >= objc)
                    return Oradpi_SetError(ip, NULL, -1, "-getmode requires a value");
                int modeIdx;
                if (Tcl_GetIndexFromObj(ip, objv[++i], getmodeNames, "getmode", 0, &modeIdx) != TCL_OK)
                    return TCL_ERROR;
                getmode = getmodeValues[modeIdx];
                break;
            }
            case LOPT_FAILOVERCB:
                if (i + 1 >= objc)
                    return Oradpi_SetError(ip, NULL, -1, "-failovercallback requires a command");
                failoverCb = objv[++i];
                break;
        }
    }

    const char *user = NULL, *pw = NULL, *db = NULL;
    uint32_t ulen = 0, plen = 0, dblen = 0;
    int ext = 0;
    Oradpi_ParseConnect(connstr, &user, &ulen, &pw, &plen, &db, &dblen, &ext);

    dpiContext* ctx = Oradpi_GetDpiContext();
    if (!ctx)
        return Oradpi_SetError(ip, NULL, -1, "ODPI context is not initialized");

    dpiCommonCreateParams cparams;
    dpiConnCreateParams ccp;
    dpiContext_initCommonCreateParams(ctx, &cparams);
    dpiContext_initConnCreateParams(ctx, &ccp);
    ccp.externalAuth = ext;
    /* Always enable threaded mode so OCI protects internal structures when
     * async worker threads operate on connections from this context. */
    cparams.createMode |= DPI_MODE_CREATE_THREADED;

    dpiConn* conn = NULL;
    dpiPool* pool = NULL;
    if (usePool)
    {
        if (minS < 0 || maxS <= 0 || incS <= 0 || maxS > UINT32_MAX || incS > UINT32_MAX || minS > UINT32_MAX)
            return Oradpi_SetError(ip, NULL, -1, "-pool: min must be >= 0, max and increment must be > 0 and <= 4294967295");
        if (minS > maxS)
            return Oradpi_SetError(ip, NULL, -1, "-pool: min must be <= max");

        dpiPoolCreateParams pp;
        dpiContext_initPoolCreateParams(ctx, &pp);
        pp.minSessions = (uint32_t)minS;
        pp.maxSessions = (uint32_t)maxS;
        pp.sessionIncrement = (uint32_t)incS;
        pp.homogeneous = homogeneous;
        pp.externalAuth = ext;
        if (dpiPool_create(ctx, user, ulen, pw, plen, db, dblen, &cparams, &pp, &pool) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_create");
        if (dpiPool_setGetMode(pool, (dpiPoolGetMode)getmode) != DPI_SUCCESS)
        {
            dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
            dpiPool_release(pool);
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_setGetMode");
        }
        if (dpiPool_acquireConnection(pool, NULL, 0, NULL, 0, &ccp, &conn) != DPI_SUCCESS)
        {
            dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
            dpiPool_release(pool);
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_acquireConnection");
        }
    }
    else
    {
        if (dpiConn_create(ctx, user, ulen, pw, plen, db, dblen, &cparams, &ccp, &conn) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiConn_create");
    }

    OradpiConn* co = Oradpi_NewConn(ip, conn, pool);
    if (!co)
    {
        if (conn)
        {
            dpiConn_close(conn, DPI_MODE_CONN_CLOSE_DEFAULT, NULL, 0);
            dpiConn_release(conn);
        }
        if (pool)
        {
            dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
            dpiPool_release(pool);
        }
        return Oradpi_SetError(ip, NULL, -1, "failed to allocate logon handle");
    }

    co->ownerIp = ip;
    co->ownerTid = Tcl_GetCurrentThread();
    co->failoverCallback = NULL;
    co->foDebounceMs = 250;
    co->foTimer = NULL;
    co->foTimerScheduled = 0;
    co->foPendingMsg = NULL;
    if (failoverCb)
    {
        co->failoverCallback = failoverCb;
        Tcl_IncrRefCount(co->failoverCallback);
    }

    Tcl_SetObjResult(ip, co->base.name);
    return TCL_OK;
}

/*
 * oralogoff logon-handle
 *
 *   Closes the connection and releases all associated statements.
 *   Returns: 0 on success.
 *   Errors:  invalid handle.
 *   Thread-safety: safe — cancels async ops, modifies per-interp state only.
 */
int Oradpi_Cmd_Logoff(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }

    OradpiConn* co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
    {
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    }

    /* Cancel all pending async operations on this connection first */
    Oradpi_CancelAndJoinAllForConn(ip, co);

    /* M-5 fix: Collect and fully remove all statements owned by this connection.
     * The old code manually closed stmt handles but left the OradpiStmt structs
     * in the stmts hash table, causing double cleanup during interp teardown.
     * We collect pointers first since Oradpi_RemoveStmt modifies the hash. */
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st)
    {
        Tcl_Size stmtCap = 8, stmtCount = 0;
        OradpiStmt** stmtsToFree = (OradpiStmt**)Tcl_Alloc(sizeof(OradpiStmt*) * (size_t)stmtCap);

        Tcl_HashSearch sSearch;
        Tcl_HashEntry* sEntry;
        for (sEntry = Tcl_FirstHashEntry(&st->stmts, &sSearch); sEntry; sEntry = Tcl_NextHashEntry(&sSearch))
        {
            OradpiStmt* s = (OradpiStmt*)Tcl_GetHashValue(sEntry);
            if (s && s->owner == co)
            {
                if (stmtCount == stmtCap)
                {
                    stmtCap *= 2;
                    stmtsToFree = (OradpiStmt**)Tcl_Realloc((char*)stmtsToFree, sizeof(OradpiStmt*) * (size_t)stmtCap);
                }
                stmtsToFree[stmtCount++] = s;
            }
        }
        for (Tcl_Size i = 0; i < stmtCount; i++)
            Oradpi_RemoveStmt(ip, stmtsToFree[i]);
        Tcl_Free((char*)stmtsToFree);

        if (co->base.name)
        {
            const char* hname = Tcl_GetString(co->base.name);
            Tcl_HashEntry* e = Tcl_FindHashEntry(&st->conns, hname);
            if (e)
                Tcl_DeleteHashEntry(e);
        }
    }

    Oradpi_FreeConn(co);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Autocommit(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle boolean");
        return TCL_ERROR;
    }
    OradpiConn* co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    int flag = 0;
    if (Tcl_GetBooleanFromObj(ip, objv[2], &flag) != TCL_OK)
        return TCL_ERROR;
    co->autocommit = flag;
    Tcl_SetObjResult(ip, Tcl_NewIntObj(flag));
    return TCL_OK;
}

int Oradpi_Cmd_Break(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
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
    if (dpiConn_breakExecution(co->conn) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiConn_breakExecution");
    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Info(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
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
    Tcl_Obj* d = Tcl_NewListObj(0, NULL);
    Tcl_ListObjAppendElement(ip, d, Tcl_NewStringObj("autocommit", -1));
    Tcl_ListObjAppendElement(ip, d, Tcl_NewBooleanObj(co->autocommit));
    Tcl_SetObjResult(ip, d);
    return TCL_OK;
}
