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
static int Oradpi_ParseConnect(const char* cs,
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
 * narrowing to uint32_t, preventing truncation on pathological inputs.
 * V-6 fix: reject overflow instead of silently clamping — returns 0 on
 * success, -1 if any component exceeds uint32_t range. */
static int Oradpi_ParseConnect(const char* cs,
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
        return 0;
    const char* slash = strchr(cs, '/');

    /* Helper macro: safely narrow a size_t to uint32_t, failing on overflow */
#define SAFE_U32(dst, sz)                                                                                                        \
    do                                                                                                                           \
    {                                                                                                                            \
        size_t _v = (sz);                                                                                                        \
        if (_v > UINT32_MAX)                                                                                                     \
            return -1;                                                                                                           \
        (dst) = (uint32_t)_v;                                                                                                    \
    } while (0)

    /* External auth: starts with / */
    if (cs[0] == '/' && (!slash || slash == cs))
    {
        *extAuth = 1;
        const char* at = strchr(cs, '@');
        if (at && at[1])
        {
            *db = at + 1;
            SAFE_U32(*dblen, strlen(*db));
        }
        return 0;
    }

    /* Look for quoted password: user/"..."@db */
    if (slash && slash[1] == '"')
    {
        *user = cs;
        SAFE_U32(*ulen, (size_t)(slash - cs));
        const char* pwStart = slash + 2; /* skip /" */
        const char* closeQuote = strchr(pwStart, '"');
        if (closeQuote)
        {
            *pw = pwStart;
            SAFE_U32(*plen, (size_t)(closeQuote - pwStart));
            /* After closing quote, expect @ or end of string */
            if (closeQuote[1] == '@' && closeQuote[2])
            {
                *db = closeQuote + 2;
                SAFE_U32(*dblen, strlen(*db));
            }
        }
        else
        {
            /* V-8 fix: Reject missing closing quote instead of silently
             * treating the rest of the string as the password.  The old
             * behavior was ambiguous and error-prone for production use. */
            return -1;
        }
        return 0;
    }

    /* Unquoted: original logic using first @ and first / */
    const char* at = strchr(cs, '@');
    if (at)
    {
        if (slash && slash < at)
        {
            *user = cs;
            SAFE_U32(*ulen, (size_t)(slash - cs));
            *pw = slash + 1;
            SAFE_U32(*plen, (size_t)(at - slash - 1));
        }
        else
        {
            *user = cs;
            SAFE_U32(*ulen, (size_t)(at - cs));
        }
        *db = at + 1;
        SAFE_U32(*dblen, strlen(*db));
    }
    else
    {
        if (slash)
        {
            *user = cs;
            SAFE_U32(*ulen, (size_t)(slash - cs));
            *pw = slash + 1;
            SAFE_U32(*plen, strlen(*pw));
        }
        else
        {
            *user = cs;
            SAFE_U32(*ulen, strlen(cs));
        }
    }
#undef SAFE_U32
    return 0;
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
    /* V-6 fix: reject pathological connect strings whose components
     * exceed uint32_t range instead of silently clamping. */
    if (Oradpi_ParseConnect(connstr, &user, &ulen, &pw, &plen, &db, &dblen, &ext) != 0)
        return Oradpi_SetError(ip, NULL, -1, "malformed connect string (missing closing quote or component exceeds maximum length)");

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

    /* Tcl 9 Tcl_Alloc panics on OOM; NewConn cannot return NULL. */
    OradpiConn* co = Oradpi_NewConn(ip, conn, pool);

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

    /* V-8 fix: Free LOBs associated with this connection before closing it.
     * Without this, oralogoff left open LOB handles in st->lobs pointing at
     * dpiLob* objects backed by a now-closed connection.  Mirrors the phased
     * cleanup order in Oradpi_DeleteInterpData: async → LOBs → stmts → conn. */
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st)
    {
        Tcl_Size lobCap = 8, lobCount = 0;
        size_t lobBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, lobCap, sizeof(OradpiLob*), &lobBytes, "LOB teardown table") != TCL_OK)
            return TCL_ERROR;
        OradpiLob** lobsToFree = (OradpiLob**)Tcl_Alloc(lobBytes);

        Tcl_HashSearch lSearch;
        Tcl_HashEntry* lEntry;
        for (lEntry = Tcl_FirstHashEntry(&st->lobs, &lSearch); lEntry; lEntry = Tcl_NextHashEntry(&lSearch))
        {
            OradpiLob* l = (OradpiLob*)Tcl_GetHashValue(lEntry);
            if (l && l->shared == co->shared)
            {
                if (lobCount == lobCap)
                {
                    if (lobCap > TCL_SIZE_MAX / 2)
                    {
                        Tcl_Free((char*)lobsToFree);
                        return Oradpi_SetError(ip, (OradpiBase*)co, -1, "LOB teardown table is too large");
                    }
                    Tcl_Size newCap = lobCap * 2;
                    size_t newBytes = 0;
                    if (Oradpi_CheckedAllocBytes(ip, newCap, sizeof(OradpiLob*), &newBytes, "LOB teardown table") != TCL_OK)
                    {
                        Tcl_Free((char*)lobsToFree);
                        return TCL_ERROR;
                    }
                    lobsToFree = (OradpiLob**)Tcl_Realloc((char*)lobsToFree, newBytes);
                    lobCap = newCap;
                }
                lobsToFree[lobCount++] = l;
            }
        }
        for (Tcl_Size i = 0; i < lobCount; i++)
            Oradpi_RemoveLob(ip, lobsToFree[i]);
        Tcl_Free((char*)lobsToFree);
    }

    /* M-5 fix: Collect and fully remove all statements owned by this connection.
     * The old code manually closed stmt handles but left the OradpiStmt structs
     * in the stmts hash table, causing double cleanup during interp teardown.
     * We collect pointers first since Oradpi_RemoveStmt modifies the hash. */
    if (st)
    {
        Tcl_Size stmtCap = 8, stmtCount = 0;
        size_t stmtBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, stmtCap, sizeof(OradpiStmt*), &stmtBytes, "statement teardown table") != TCL_OK)
            return TCL_ERROR;
        OradpiStmt** stmtsToFree = (OradpiStmt**)Tcl_Alloc(stmtBytes);

        Tcl_HashSearch sSearch;
        Tcl_HashEntry* sEntry;
        for (sEntry = Tcl_FirstHashEntry(&st->stmts, &sSearch); sEntry; sEntry = Tcl_NextHashEntry(&sSearch))
        {
            OradpiStmt* s = (OradpiStmt*)Tcl_GetHashValue(sEntry);
            if (s && s->owner == co)
            {
                if (stmtCount == stmtCap)
                {
                    Tcl_Size newCap = 0;
                    size_t stmtBytes = 0;
                    if (stmtCap > TCL_SIZE_MAX / 2)
                    {
                        Tcl_Free((char*)stmtsToFree);
                        return Oradpi_SetError(ip, (OradpiBase*)co, -1, "statement teardown table is too large");
                    }
                    newCap = stmtCap * 2;
                    if (Oradpi_CheckedAllocBytes(ip, newCap, sizeof(OradpiStmt*), &stmtBytes, "statement teardown table") !=
                        TCL_OK)
                    {
                        Tcl_Free((char*)stmtsToFree);
                        return TCL_ERROR;
                    }
                    stmtsToFree = (OradpiStmt**)Tcl_Realloc((char*)stmtsToFree, stmtBytes);
                    stmtCap = newCap;
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
    CONN_BREAK(co);
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
    Tcl_IncrRefCount(d);
    LAPPEND_CHK(ip, d, Tcl_NewStringObj("autocommit", -1));
    LAPPEND_CHK(ip, d, Tcl_NewBooleanObj(co->autocommit));
    Tcl_SetObjResult(ip, d);
    Tcl_DecrRefCount(d);
    return TCL_OK;
}
