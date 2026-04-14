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
#include <stdint.h>
#include <string.h>

#include "cmd_int.h"

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

int        Oradpi_Cmd_Autocommit(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int        Oradpi_Cmd_Break(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int        Oradpi_Cmd_Info(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int        Oradpi_Cmd_Logoff(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int        Oradpi_Cmd_Logon(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
static int Oradpi_ParseConnect(const char *cs, const char **user, uint32_t *ulen, const char **pw, uint32_t *plen, const char **db, uint32_t *dblen, int *extAuth, Tcl_DString *pwDs);

/* ==========================================================================
 * Process-global shared pool registry
 *
 * Maps a canonical key (connstr + sizing + tuning params) → dpiPool*.
 * Multiple oralogon -pool calls with identical parameters reuse one dpiPool
 * instead of creating independent pools.  Benefits:
 *   - One shared statement cache per pool instead of N fragmented caches
 *   - Idle sessions from one handle can be reused by another
 *   - Pool creation work (OCI connection to server) is paid once
 *
 * Lifecycle:
 *   PoolRegistry_Acquire   – lookup-or-create; returns addRef'd dpiPool*
 *   PoolRegistry_Release   – dpiPool_release; removes entry when refCount==0
 *   Both are called under gPoolMapMutex (leaf lock, no other locks held).
 *
 * Pool tuning knobs (waitTimeout, timeout, etc.) are part of the registry
 * key so that two calls with different tuning parameters get distinct pools.
 * ========================================================================== */

typedef struct PoolEntry {
    dpiPool     *pool;
    unsigned int refCount;
    char        *key; /* owned copy of the lookup key */
} PoolEntry;

static Tcl_Mutex     gPoolMapMutex;
static int           gPoolMapInited = 0;
static Tcl_HashTable gPoolMap;

static void          PoolRegistry_ExitHandler(void *unused) {
    (void)unused;
    Tcl_MutexLock(&gPoolMapMutex);
    if (gPoolMapInited) {
        Tcl_HashSearch s;
        Tcl_HashEntry *he;
        for (he = Tcl_FirstHashEntry(&gPoolMap, &s); he; he = Tcl_NextHashEntry(&s)) {
            PoolEntry *pe = (PoolEntry *)Tcl_GetHashValue(he);
            if (pe) {
                if (pe->pool) {
                    dpiPool_close(pe->pool, DPI_MODE_POOL_CLOSE_DEFAULT);
                    dpiPool_release(pe->pool);
                }
                if (pe->key)
                    Tcl_Free(pe->key);
                Tcl_Free((char *)pe);
            }
        }
        Tcl_DeleteHashTable(&gPoolMap);
        gPoolMapInited = 0;
    }
    Tcl_MutexUnlock(&gPoolMapMutex);
}

static void PoolRegistry_Init(void) {
    Tcl_MutexLock(&gPoolMapMutex);
    if (!gPoolMapInited) {
        Tcl_InitHashTable(&gPoolMap, TCL_STRING_KEYS);
        gPoolMapInited = 1;
        Tcl_CreateExitHandler(PoolRegistry_ExitHandler, NULL);
    }
    Tcl_MutexUnlock(&gPoolMapMutex);
}

/* Look up or create a pool for the given key.
 * On success: returns a dpiPool* that has been addRef'd on behalf of the
 * caller (either by dpiPool_addRef on an existing pool, or owned from
 * dpiPool_create for a new one); also increments pe->refCount.
 * On failure: returns NULL; does not modify the registry. */
static dpiPool *PoolRegistry_Acquire(dpiContext *ctx, const char *key, const char *user, uint32_t ulen, const char *pw, uint32_t plen, const char *db, uint32_t dblen, dpiCommonCreateParams *cparams,
                                     dpiPoolCreateParams *pp, int getmode) {
    PoolRegistry_Init();

    Tcl_MutexLock(&gPoolMapMutex);
    int            isNew = 0;
    Tcl_HashEntry *he    = Tcl_CreateHashEntry(&gPoolMap, key, &isNew);
    PoolEntry     *pe    = isNew ? NULL : (PoolEntry *)Tcl_GetHashValue(he);

    if (!isNew && pe && pe->pool) {
        /* Existing pool — addRef so the caller holds an independent ref */
        if (dpiPool_addRef(pe->pool) != DPI_SUCCESS) {
            Tcl_MutexUnlock(&gPoolMapMutex);
            return NULL;
        }
        pe->refCount++;
        dpiPool *p = pe->pool;
        Tcl_MutexUnlock(&gPoolMapMutex);
        return p;
    }

    /* New entry — must create the pool outside the lock to avoid holding
     * gPoolMapMutex during a potential network round-trip. */
    Tcl_MutexUnlock(&gPoolMapMutex);

    dpiPool *pool = NULL;
    if (dpiPool_create(ctx, user, ulen, pw, plen, db, dblen, cparams, pp, &pool) != DPI_SUCCESS)
        return NULL;
    if (dpiPool_setGetMode(pool, (dpiPoolGetMode)getmode) != DPI_SUCCESS) {
        dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
        dpiPool_release(pool);
        return NULL;
    }

    /* Re-acquire lock and insert.  Another thread might have raced us and
     * already created a pool for the same key.  If so, prefer the existing
     * one: close ours and addRef theirs. */
    Tcl_MutexLock(&gPoolMapMutex);
    isNew = 0;
    he    = Tcl_CreateHashEntry(&gPoolMap, key, &isNew);
    pe    = isNew ? NULL : (PoolEntry *)Tcl_GetHashValue(he);

    if (!isNew && pe && pe->pool) {
        /* Lost the race — use the winner's pool */
        dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
        dpiPool_release(pool);
        if (dpiPool_addRef(pe->pool) != DPI_SUCCESS) {
            Tcl_MutexUnlock(&gPoolMapMutex);
            return NULL;
        }
        pe->refCount++;
        pool = pe->pool;
        Tcl_MutexUnlock(&gPoolMapMutex);
        return pool;
    }

    /* We won — publish */
    pe           = (PoolEntry *)Tcl_Alloc(sizeof(*pe));
    pe->pool     = pool;
    pe->refCount = 1;
    size_t klen  = strlen(key) + 1;
    pe->key      = (char *)Tcl_Alloc(klen);
    memcpy(pe->key, key, klen);
    Tcl_SetHashValue(he, pe);
    /* Caller gets the pool* we just created — already has refCount 1 from
     * dpiPool_create; no additional addRef needed. */
    Tcl_MutexUnlock(&gPoolMapMutex);
    return pool;
}

/* Release one reference to the pool for key.
 * When refCount reaches 0, closes and removes the pool from the registry.
 * Also always calls dpiPool_release to drop the caller's addRef'd ref. */
static void PoolRegistry_Release(const char *key, dpiPool *pool) {
    if (!pool)
        return;

    Tcl_MutexLock(&gPoolMapMutex);
    int doClose = 0;
    if (gPoolMapInited) {
        Tcl_HashEntry *he = Tcl_FindHashEntry(&gPoolMap, key);
        if (he) {
            PoolEntry *pe = (PoolEntry *)Tcl_GetHashValue(he);
            if (pe && pe->refCount > 0)
                pe->refCount--;
            if (pe && pe->refCount == 0) {
                doClose = 1;
                if (pe->key)
                    Tcl_Free(pe->key);
                Tcl_Free((char *)pe);
                Tcl_DeleteHashEntry(he);
            }
        }
    }
    Tcl_MutexUnlock(&gPoolMapMutex);

    if (doClose)
        dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
    dpiPool_release(pool);
}

/* Build the canonical registry key from pool parameters.
 * Key format: "user@db|min:max:incr:hom:ext|wait:to:life:ping:pingto:sc"
 * Passwords are intentionally excluded from the key — they are credentials,
 * not pool-identity parameters, and different callers sharing a pool may
 * use different authentication wrappers. */
static void PoolRegistry_BuildKey(Tcl_DString *ds, const char *user, uint32_t ulen, const char *db, uint32_t dblen, Tcl_WideInt minS, Tcl_WideInt maxS, Tcl_WideInt incS, int homogeneous, int ext,
                                  Tcl_WideInt waitTimeout, Tcl_WideInt timeout, Tcl_WideInt maxLifetime, Tcl_WideInt pingInterval, Tcl_WideInt pingTimeout, Tcl_WideInt stmtCacheSize) {
    Tcl_DStringInit(ds);
    Tcl_DStringAppend(ds, user ? user : "", (Tcl_Size)ulen);
    Tcl_DStringAppend(ds, "@", 1);
    Tcl_DStringAppend(ds, db ? db : "", (Tcl_Size)dblen);
    char buf[128];
    snprintf(buf, sizeof(buf),
             "|%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d:%d:%d"
             "|%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d:%" TCL_LL_MODIFIER "d",
             minS, maxS, incS, homogeneous, ext, waitTimeout, timeout, maxLifetime, pingInterval, pingTimeout, stmtCacheSize);
    Tcl_DStringAppend(ds, buf, -1);
}

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/* Parse connect string with support for double-quoted passwords.
 * Format: user/"pass@word"@db  or  user/password@db  or  /  (ext auth)
 * Passwords containing @ or / must be double-quoted.
 * quoted passwords now support backslash escapes:
 *   \"  → literal double-quote inside the password
 *   \\  → literal backslash
 * Without escaping, passwords containing " were unrepresentable.
 * all length computations use size_t with range checks before
 * narrowing to uint32_t, preventing truncation on pathological inputs.
 * reject overflow instead of silently clamping — returns 0 on
 * success, -1 if any component exceeds uint32_t range. */
static int Oradpi_ParseConnect(const char *cs, const char **user, uint32_t *ulen, const char **pw, uint32_t *plen, const char **db, uint32_t *dblen, int *extAuth, Tcl_DString *pwDs) {
    *user = *pw = *db = NULL;
    *ulen = *plen = *dblen = 0;
    *extAuth               = 0;
    if (!cs)
        return 0;
    const char *slash = strchr(cs, '/');

    /* Helper macro: safely narrow a size_t to uint32_t, failing on overflow */
#define SAFE_U32(dst, sz)                                                                                                                                                                              \
    do {                                                                                                                                                                                               \
        size_t _v = (sz);                                                                                                                                                                              \
        if (_v > UINT32_MAX)                                                                                                                                                                           \
            return -1;                                                                                                                                                                                 \
        (dst) = (uint32_t)_v;                                                                                                                                                                          \
    } while (0)

    /* External auth: starts with / */
    if (cs[0] == '/' && (!slash || slash == cs)) {
        *extAuth       = 1;
        const char *at = strchr(cs, '@');
        if (at && at[1]) {
            *db = at + 1;
            SAFE_U32(*dblen, strlen(*db));
        }
        return 0;
    }

    /* Look for quoted password: user/"..."@db */
    if (slash && slash[1] == '"') {
        *user = cs;
        SAFE_U32(*ulen, (size_t)(slash - cs));
        const char *pwStart    = slash + 2; /* skip /" */

        /* scan forward honouring backslash escapes to find the real
         * closing quote.  \" and \\ are the two recognised escape sequences;
         * any other \X is left as-is (not an error). */
        const char *p          = pwStart;
        size_t      decodedLen = 0;
        while (*p && *p != '"') {
            if (*p == '\\' && (p[1] == '"' || p[1] == '\\'))
                p++; /* skip the backslash; count only the next char */
            p++;
            decodedLen++;
        }
        if (*p != '"') {
            /* reject missing closing quote */
            return -1;
        }
        const char *closeQuote = p;

        /* If there are no escape sequences the fast path applies: point
         * directly into the original string just as before. */
        if (decodedLen == (size_t)(closeQuote - pwStart)) {
            /* No escapes — safe to alias into the original buffer */
            *pw = pwStart;
            SAFE_U32(*plen, decodedLen);
        } else {
            /* Escapes present — decode into the caller-supplied Tcl_DString.
             * This avoids the former static buffer, which was unsafe under
             * concurrent oralogon calls (cross-thread data race on credentials).
             * Tcl_DString is stack-anchored for short strings; no heap alloc
             * unless the decoded password exceeds TCL_DSTRING_STATIC_SIZE. */
            size_t rawSpan = (size_t)(closeQuote - pwStart);
            /* ODPI-C takes uint32_t length; reject overlarge passwords */
            if (rawSpan > UINT32_MAX)
                return -1;
            const char *src = pwStart;
            while (*src && *src != '"') {
                char ch = *src;
                if (ch == '\\' && (src[1] == '"' || src[1] == '\\'))
                    ch = *++src; /* consume backslash; use escaped char */
                Tcl_DStringAppend(pwDs, &ch, 1);
                src++;
            }
            *pw = Tcl_DStringValue(pwDs);
            SAFE_U32(*plen, (size_t)Tcl_DStringLength(pwDs));
        }

        /* After closing quote, expect @ or end of string */
        if (closeQuote[1] == '@' && closeQuote[2]) {
            *db = closeQuote + 2;
            SAFE_U32(*dblen, strlen(*db));
        }
        return 0;
    }

    /* Unquoted: original logic using first @ and first / */
    const char *at = strchr(cs, '@');
    if (at) {
        if (slash && slash < at) {
            *user = cs;
            SAFE_U32(*ulen, (size_t)(slash - cs));
            *pw = slash + 1;
            SAFE_U32(*plen, (size_t)(at - slash - 1));
        } else {
            *user = cs;
            SAFE_U32(*ulen, (size_t)(at - cs));
        }
        *db = at + 1;
        SAFE_U32(*dblen, strlen(*db));
    } else {
        if (slash) {
            *user = cs;
            SAFE_U32(*ulen, (size_t)(slash - cs));
            *pw = slash + 1;
            SAFE_U32(*plen, strlen(*pw));
        } else {
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

/* Called by state.c (SharedConnRelease / exit handler) when the last
 * connection wrapper referencing a pool drops its reference.
 * For registry-tracked pools, this decrements the refcount and lets the
 * registry close the pool when it reaches zero.
 * For pools not tracked by the registry, falls back to direct close. */
void Oradpi_PoolRelease(dpiPool *pool) {
    if (!pool)
        return;

    /* Search the registry for this pool pointer */
    int found = 0;
    Tcl_MutexLock(&gPoolMapMutex);
    if (gPoolMapInited) {
        Tcl_HashSearch s;
        Tcl_HashEntry *he;
        for (he = Tcl_FirstHashEntry(&gPoolMap, &s); he; he = Tcl_NextHashEntry(&s)) {
            PoolEntry *pe = (PoolEntry *)Tcl_GetHashValue(he);
            if (pe && pe->pool == pool) {
                found       = 1;
                int doClose = 0;
                if (pe->refCount > 0)
                    pe->refCount--;
                if (pe->refCount == 0) {
                    doClose = 1;
                    if (pe->key)
                        Tcl_Free(pe->key);
                    Tcl_Free((char *)pe);
                    Tcl_DeleteHashEntry(he);
                }
                Tcl_MutexUnlock(&gPoolMapMutex);
                if (doClose)
                    dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
                dpiPool_release(pool);
                return;
            }
        }
    }
    Tcl_MutexUnlock(&gPoolMapMutex);

    /* Pool not found in registry — close and release it directly */
    if (!found) {
        dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
        dpiPool_release(pool);
    }
}

int Oradpi_Cmd_Logon(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv,
                         "connect-str ?-pool min max incr? ?-homogeneous bool? "
                         "?-getmode wait|nowait|forceget|timedwait? "
                         "?-failovercallback proc?");
        return TCL_ERROR;
    }
    const char              *connstr = Tcl_GetString(objv[1]);
    int                      usePool = 0, homogeneous = 1;
    Tcl_WideInt              minS = 1, maxS = 4, incS = 1;
    int                      getmode           = DPI_MODE_POOL_GET_WAIT;
    Tcl_Obj                 *failoverCb        = NULL;
    /* Pool tuning knobs; -1 means keep the ODPI default */
    Tcl_WideInt              poolWaitTimeout   = -1; /* ms; only used with timedwait getmode */
    Tcl_WideInt              poolTimeout       = -1; /* s;  idle session eviction */
    Tcl_WideInt              poolMaxLifetime   = -1; /* s;  max session age */
    Tcl_WideInt              poolPingInterval  = -1; /* s;  0 = disable liveness ping */
    Tcl_WideInt              poolPingTimeout   = -1; /* ms; ping timeout */
    Tcl_WideInt              poolStmtCacheSize = -1; /* per-session statement cache size */

    static const char *const logonOpts[]       = {"-pool",        "-homogeneous",   "-getmode", "-failovercallback", "-waittimeout", "-timeout", "-maxlifetime", "-pinginterval",
                                                  "-pingtimeout", "-stmtcachesize", NULL};
    enum LogonOptIdx { LOPT_POOL, LOPT_HOMOGENEOUS, LOPT_GETMODE, LOPT_FAILOVERCB, LOPT_WAITTIMEOUT, LOPT_TIMEOUT, LOPT_MAXLIFETIME, LOPT_PINGINTERVAL, LOPT_PINGTIMEOUT, LOPT_STMTCACHESIZE };

    static const char *const getmodeNames[]  = {"wait", "nowait", "forceget", "timedwait", NULL};
    static const int         getmodeValues[] = {DPI_MODE_POOL_GET_WAIT, DPI_MODE_POOL_GET_NOWAIT, DPI_MODE_POOL_GET_FORCEGET, DPI_MODE_POOL_GET_TIMEDWAIT};

    for (Tcl_Size i = 2; i < objc; i++) {
        int optIdx;
        if (Tcl_GetIndexFromObj(ip, objv[i], logonOpts, "option", 0, &optIdx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum LogonOptIdx)optIdx) {
        case LOPT_POOL: {
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-pool requires arguments");
            Tcl_Size  n     = 0;
            Tcl_Obj **elems = NULL;
            if (Tcl_ListObjGetElements(ip, objv[i + 1], &n, &elems) == TCL_OK && n == 3) {
                i++;
                if (Tcl_GetWideIntFromObj(ip, elems[0], &minS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetWideIntFromObj(ip, elems[1], &maxS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetWideIntFromObj(ip, elems[2], &incS) != TCL_OK)
                    return TCL_ERROR;
            } else if (i + 3 < objc) {
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &minS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &maxS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &incS) != TCL_OK)
                    return TCL_ERROR;
            } else
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
        case LOPT_GETMODE: {
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
        /* Pool tuning knobs */
        case LOPT_WAITTIMEOUT:
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-waittimeout requires a value (ms)");
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &poolWaitTimeout) != TCL_OK)
                return TCL_ERROR;
            break;
        case LOPT_TIMEOUT:
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-timeout requires a value (s)");
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &poolTimeout) != TCL_OK)
                return TCL_ERROR;
            break;
        case LOPT_MAXLIFETIME:
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-maxlifetime requires a value (s)");
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &poolMaxLifetime) != TCL_OK)
                return TCL_ERROR;
            break;
        case LOPT_PINGINTERVAL:
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-pinginterval requires a value (s)");
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &poolPingInterval) != TCL_OK)
                return TCL_ERROR;
            break;
        case LOPT_PINGTIMEOUT:
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-pingtimeout requires a value (ms)");
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &poolPingTimeout) != TCL_OK)
                return TCL_ERROR;
            break;
        case LOPT_STMTCACHESIZE:
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "-stmtcachesize requires a value");
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &poolStmtCacheSize) != TCL_OK)
                return TCL_ERROR;
            break;
        }
    }

    const char *user = NULL, *pw = NULL, *db = NULL;
    uint32_t    ulen = 0, plen = 0, dblen = 0;
    int         ext = 0;
    /* pwDs holds the unescaped password bytes when the connect string uses
     * backslash escapes inside a quoted password.  It must be live until
     * after the dpiConn_create / dpiPool_create calls that read pw-plen,
     * then freed immediately afterwards to avoid leaking its storage. */
    Tcl_DString pwDs;
    Tcl_DStringInit(&pwDs);
    /* reject pathological connect strings whose components
     * exceed uint32_t range instead of silently clamping. */
    if (Oradpi_ParseConnect(connstr, &user, &ulen, &pw, &plen, &db, &dblen, &ext, &pwDs) != 0) {
        Tcl_DStringFree(&pwDs);
        return Oradpi_SetError(ip, NULL, -1, "malformed connect string (missing closing quote or component exceeds maximum length)");
    }

    dpiContext *ctx = Oradpi_GetDpiContext();
    if (!ctx)
        return Oradpi_SetError(ip, NULL, -1, "ODPI context is not initialized");

    dpiCommonCreateParams cparams;
    dpiConnCreateParams   ccp;
    dpiContext_initCommonCreateParams(ctx, &cparams);
    dpiContext_initConnCreateParams(ctx, &ccp);
    ccp.externalAuth = ext;
    /* Always enable threaded mode so OCI protects internal structures when
     * async worker threads operate on connections from this context. */
    cparams.createMode |= DPI_MODE_CREATE_THREADED;

    dpiConn *conn = NULL;
    dpiPool *pool = NULL;
    if (usePool) {
        if (minS < 0 || maxS <= 0 || incS <= 0 || maxS > UINT32_MAX || incS > UINT32_MAX || minS > UINT32_MAX)
            return Oradpi_SetError(ip, NULL, -1, "-pool: min must be >= 0, max and increment must be > 0 and <= 4294967295");
        if (minS > maxS)
            return Oradpi_SetError(ip, NULL, -1, "-pool: min must be <= max");

        dpiPoolCreateParams pp;
        dpiContext_initPoolCreateParams(ctx, &pp);
        pp.minSessions      = (uint32_t)minS;
        pp.maxSessions      = (uint32_t)maxS;
        pp.sessionIncrement = (uint32_t)incS;
        pp.homogeneous      = homogeneous;
        pp.externalAuth     = ext;
        /* Apply optional pool tuning knobs when set by the caller */
        if (poolWaitTimeout >= 0)
            pp.waitTimeout = (uint32_t)poolWaitTimeout;
        if (poolTimeout >= 0)
            pp.timeout = (uint32_t)poolTimeout;
        if (poolMaxLifetime >= 0)
            pp.maxLifetimeSession = (uint32_t)poolMaxLifetime;
        if (poolPingInterval >= 0)
            pp.pingInterval = (int)poolPingInterval;
        if (poolPingTimeout >= 0)
            pp.pingTimeout = (uint32_t)poolPingTimeout;
        /* stmtCacheSize lives on dpiCommonCreateParams, not dpiPoolCreateParams */
        if (poolStmtCacheSize >= 0)
            cparams.stmtCacheSize = (uint32_t)poolStmtCacheSize;

        /* Look up or create a shared pool for this parameter combination.
         * Multiple oralogon -pool calls with the same parameters share one dpiPool*. */
        Tcl_DString poolKey;
        PoolRegistry_BuildKey(&poolKey, user, ulen, db, dblen, minS, maxS, incS, homogeneous, ext, poolWaitTimeout, poolTimeout, poolMaxLifetime, poolPingInterval, poolPingTimeout, poolStmtCacheSize);

        pool = PoolRegistry_Acquire(ctx, Tcl_DStringValue(&poolKey), user, ulen, pw, plen, db, dblen, &cparams, &pp, getmode);
        Tcl_DStringFree(&poolKey);

        if (!pool) {
            Tcl_DStringFree(&pwDs);
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_create");
        }

        if (dpiPool_acquireConnection(pool, NULL, 0, NULL, 0, &ccp, &conn) != DPI_SUCCESS) {
            /* Release our registry ref; pool stays alive if other handles share it */
            Tcl_DString releaseKey;
            PoolRegistry_BuildKey(&releaseKey, user, ulen, db, dblen, minS, maxS, incS, homogeneous, ext, poolWaitTimeout, poolTimeout, poolMaxLifetime, poolPingInterval, poolPingTimeout,
                                  poolStmtCacheSize);
            PoolRegistry_Release(Tcl_DStringValue(&releaseKey), pool);
            Tcl_DStringFree(&releaseKey);
            pool = NULL;
            Tcl_DStringFree(&pwDs);
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_acquireConnection");
        }
    } else {
        if (dpiConn_create(ctx, user, ulen, pw, plen, db, dblen, &cparams, &ccp, &conn) != DPI_SUCCESS) {
            Tcl_DStringFree(&pwDs);
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiConn_create");
        }
    }

    /* Password storage no longer needed — ODPI has copied credentials */
    Tcl_DStringFree(&pwDs);

    /* Tcl 9 Tcl_Alloc panics on OOM; NewConn cannot return NULL. */
    OradpiConn *co       = Oradpi_NewConn(ip, conn, pool);

    co->ownerIp          = ip;
    co->ownerTid         = Tcl_GetCurrentThread();
    co->failoverCallback = NULL;
    co->foDebounceMs     = 250;
    co->foTimer          = NULL;
    co->foTimerScheduled = 0;
    co->foPendingMsg     = NULL;
    if (failoverCb) {
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
int Oradpi_Cmd_Logoff(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }

    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co) {
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    }

    /* Cancel all pending async operations on this connection first */
    Oradpi_CancelAndJoinAllForConn(ip, co);

    /* Free LOBs associated with this connection before closing it.
     * LOB handles in st->lobs reference dpiLob* objects backed by this
     * connection and must be released before it is closed.  Mirrors the phased
     * cleanup order in Oradpi_DeleteInterpData: async → LOBs → stmts → conn. */
    OradpiInterpState *st = (OradpiInterpState *)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st) {
        Tcl_Size lobCap = 8, lobCount = 0;
        size_t   lobBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, lobCap, sizeof(OradpiLob *), &lobBytes, "LOB teardown table") != TCL_OK)
            return TCL_ERROR;
        OradpiLob    **lobsToFree = (OradpiLob **)Tcl_Alloc(lobBytes);

        Tcl_HashSearch lSearch;
        Tcl_HashEntry *lEntry;
        for (lEntry = Tcl_FirstHashEntry(&st->lobs, &lSearch); lEntry; lEntry = Tcl_NextHashEntry(&lSearch)) {
            OradpiLob *l = (OradpiLob *)Tcl_GetHashValue(lEntry);
            if (l && l->shared == co->shared) {
                if (lobCount == lobCap) {
                    if (lobCap > TCL_SIZE_MAX / 2) {
                        Tcl_Free((char *)lobsToFree);
                        return Oradpi_SetError(ip, (OradpiBase *)co, -1, "LOB teardown table is too large");
                    }
                    Tcl_Size newCap   = lobCap * 2;
                    size_t   newBytes = 0;
                    if (Oradpi_CheckedAllocBytes(ip, newCap, sizeof(OradpiLob *), &newBytes, "LOB teardown table") != TCL_OK) {
                        Tcl_Free((char *)lobsToFree);
                        return TCL_ERROR;
                    }
                    lobsToFree = (OradpiLob **)Tcl_Realloc((char *)lobsToFree, newBytes);
                    lobCap     = newCap;
                }
                lobsToFree[lobCount++] = l;
            }
        }
        for (Tcl_Size i = 0; i < lobCount; i++)
            Oradpi_RemoveLob(ip, lobsToFree[i]);
        Tcl_Free((char *)lobsToFree);
    }

    /* Collect and fully remove all statements owned by this connection.
     * Each statement must be fully removed (hash entry + struct) to prevent
     * double cleanup during interp teardown.
     * We collect pointers first since Oradpi_RemoveStmt modifies the hash. */
    if (st) {
        Tcl_Size stmtCap = 8, stmtCount = 0;
        size_t   stmtBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, stmtCap, sizeof(OradpiStmt *), &stmtBytes, "statement teardown table") != TCL_OK)
            return TCL_ERROR;
        OradpiStmt   **stmtsToFree = (OradpiStmt **)Tcl_Alloc(stmtBytes);

        Tcl_HashSearch sSearch;
        Tcl_HashEntry *sEntry;
        for (sEntry = Tcl_FirstHashEntry(&st->stmts, &sSearch); sEntry; sEntry = Tcl_NextHashEntry(&sSearch)) {
            OradpiStmt *s = (OradpiStmt *)Tcl_GetHashValue(sEntry);
            if (s && s->owner == co) {
                if (stmtCount == stmtCap) {
                    Tcl_Size newCap    = 0;
                    size_t   stmtBytes = 0;
                    if (stmtCap > TCL_SIZE_MAX / 2) {
                        Tcl_Free((char *)stmtsToFree);
                        return Oradpi_SetError(ip, (OradpiBase *)co, -1, "statement teardown table is too large");
                    }
                    newCap = stmtCap * 2;
                    if (Oradpi_CheckedAllocBytes(ip, newCap, sizeof(OradpiStmt *), &stmtBytes, "statement teardown table") != TCL_OK) {
                        Tcl_Free((char *)stmtsToFree);
                        return TCL_ERROR;
                    }
                    stmtsToFree = (OradpiStmt **)Tcl_Realloc((char *)stmtsToFree, stmtBytes);
                    stmtCap     = newCap;
                }
                stmtsToFree[stmtCount++] = s;
            }
        }
        for (Tcl_Size i = 0; i < stmtCount; i++)
            Oradpi_RemoveStmt(ip, stmtsToFree[i]);
        Tcl_Free((char *)stmtsToFree);

        if (co->base.name) {
            const char    *hname = Tcl_GetString(co->base.name);
            Tcl_HashEntry *e     = Tcl_FindHashEntry(&st->conns, hname);
            if (e)
                Tcl_DeleteHashEntry(e);
        }
    }

    Oradpi_FreeConn(co);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Autocommit(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle boolean");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    int flag = 0;
    if (Tcl_GetBooleanFromObj(ip, objv[2], &flag) != TCL_OK)
        return TCL_ERROR;
    co->autocommit = flag;
    /* Propagate autocommit change to the shared adoption snapshot so
     * future adopters inherit the new value and existing wrappers on the same
     * physical session stay consistent. */
    Oradpi_SharedConnSyncBehavior(co);
    Tcl_SetObjResult(ip, Tcl_NewIntObj(flag));
    return TCL_OK;
}

int Oradpi_Cmd_Break(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    CONN_BREAK(co);
    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Info(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    Tcl_Obj *d = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(d);
    LAPPEND_CHK(ip, d, Tcl_NewStringObj("autocommit", -1));
    LAPPEND_CHK(ip, d, Tcl_NewBooleanObj(co->autocommit));
    Tcl_SetObjResult(ip, d);
    Tcl_DecrRefCount(d);
    return TCL_OK;
}
