/*
 *  state.c --
 *
 *    Shared type and state definitions for the extension.
 *
 *        - Declares handle structs for connections, statements, LOBs, and the per-interpreter state block.
 *        - Designed for multi-interp/multi-thread use: per-interp registries and reference tracking ensure
 *          safe teardown; process-wide data is protected by Tcl mutexes.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <stdatomic.h>
#include <string.h>

#include "cmd_int.h"

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static GlobalConnRec* GlobalConn_PublishAndRef(const char* name, dpiConn* conn);
static GlobalConnRec* GlobalConn_LookupForAdoptAndRef(const char* name, dpiConn** outConn, int* pOwnerAlive);
static void GlobalConn_MarkOwnerGone(GlobalConnRec* gr);
static void GlobalConnMap_Init(void);
static OradpiConn* Oradpi_AdoptConn(Tcl_Interp* ip, const char* handleName, GlobalConnRec* shared, dpiConn* connFromOwner);
void Oradpi_DeleteInterpData(void* clientData, Tcl_Interp* ip);
void Oradpi_FreeConn(OradpiConn* co);
void Oradpi_FreeMsg(OradpiMsg* m);
void Oradpi_FreeLob(OradpiLob* l);
void Oradpi_FreeStmt(Tcl_Interp* ip, OradpiStmt* s);
void Oradpi_RemoveStmt(Tcl_Interp* ip, OradpiStmt* s);
static OradpiInterpState* Oradpi_Get(Tcl_Interp* ip);
OradpiConn* Oradpi_LookupConn(Tcl_Interp* ip, Tcl_Obj* nameObj);
OradpiLob* Oradpi_LookupLob(Tcl_Interp* ip, Tcl_Obj* nameObj);
OradpiStmt* Oradpi_LookupStmt(Tcl_Interp* ip, Tcl_Obj* nameObj);
OradpiConn* Oradpi_NewConn(Tcl_Interp* ip, dpiConn* conn, dpiPool* pool);
OradpiLob* Oradpi_NewLob(Tcl_Interp* ip, dpiLob* lob, GlobalConnRec* shared);
OradpiStmt* Oradpi_NewStmt(Tcl_Interp* ip, OradpiConn* co);
static void Oradpi_RegisterConnInInterp(OradpiInterpState* st, OradpiConn* co);

/* ------------------------------------------------------------------------- *
 * Process-global and per-interpreter state
 * ------------------------------------------------------------------------- */

typedef struct GlobalConnRec
{
    _Atomic(dpiConn*) pubConn; /* published only while owner is alive */
    int ownerAlive;            /* 1 while creator interp still owns the live connection */
    unsigned int refCount;     /* wrappers + async workers */
    Tcl_Mutex connLock;        /* shared per-dpiConn operation gate */
    Tcl_Condition connCond;
    Tcl_ThreadId opOwner; /* recursive ownership for same-thread reentry */
    unsigned int opDepth;
    char* nameKey; /* owned copy for hash removal / diagnostics */

    /* V-8 fix: Behavioral policy snapshot.  Populated at publish time from
     * the owner's wrapper, and updated by oraconfig.  Adopters copy these
     * instead of using hardcoded defaults, ensuring the same dpiConn*
     * behaves consistently across interps. */
    int snap_autocommit;
    uint32_t snap_fetchArraySize;
    uint32_t snap_prefetchRows;
    int snap_inlineLobs;
    uint32_t snap_foMaxAttempts;
    uint32_t snap_foBackoffMs;
    double snap_foBackoffFactor;
    uint32_t snap_foErrorClasses;
    uint32_t snap_foDebounceMs;
} GlobalConnRec;

/* gConnMapMutex: protects gConnByName hash table and gConnMapInited flag.
 * Lock ordering: leaf lock, no other module locks held while this is held. */
static Tcl_Mutex gConnMapMutex;
static int gConnMapInited = 0;
static Tcl_HashTable gConnByName;

static char* GlobalConn_DupName(const char* name)
{
    size_t n = strlen(name) + 1u;
    char* copy = (char*)Tcl_Alloc(n);
    memcpy(copy, name, n);
    return copy;
}

/* M-3 / V-8: Cleanup handler for process exit — frees GlobalConnRec entries
 * whose refCount has reached 0.  Entries with outstanding references (e.g.
 * from orphaned async workers still alive after teardown timeout) are
 * intentionally skipped — their memory will be reclaimed by process exit.
 * Freeing them while workers still hold pointers is use-after-free. */
static void GlobalConnMap_ExitHandler(void* unused)
{
    (void)unused;
    Tcl_MutexLock(&gConnMapMutex);
    if (gConnMapInited)
    {
        Tcl_HashSearch search;
        Tcl_HashEntry* he;
        for (he = Tcl_FirstHashEntry(&gConnByName, &search); he; he = Tcl_NextHashEntry(&search))
        {
            GlobalConnRec* gr = (GlobalConnRec*)Tcl_GetHashValue(he);
            if (gr)
            {
                if (gr->refCount > 0)
                    continue; /* V-8: still referenced — skip to avoid UaF */
                Tcl_ConditionFinalize(&gr->connCond);
                Tcl_MutexFinalize(&gr->connLock);
                if (gr->nameKey)
                    Tcl_Free(gr->nameKey);
                Tcl_Free((char*)gr);
            }
        }
        Tcl_DeleteHashTable(&gConnByName);
        gConnMapInited = 0;
    }
    Tcl_MutexUnlock(&gConnMapMutex);
}

static void GlobalConnMap_Init(void)
{
    Tcl_MutexLock(&gConnMapMutex);
    if (!gConnMapInited)
    {
        Tcl_InitHashTable(&gConnByName, TCL_STRING_KEYS);
        gConnMapInited = 1;
        Tcl_CreateExitHandler(GlobalConnMap_ExitHandler, NULL);
    }
    Tcl_MutexUnlock(&gConnMapMutex);
}

static GlobalConnRec* GlobalConn_PublishAndRef(const char* name, dpiConn* conn)
{
    GlobalConnRec* gr = NULL;

    GlobalConnMap_Init();
    Tcl_MutexLock(&gConnMapMutex);
    int newEntry = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&gConnByName, name, &newEntry);
    if (newEntry)
    {
        gr = (GlobalConnRec*)Tcl_Alloc(sizeof(*gr));
        memset(gr, 0, sizeof(*gr));
        gr->nameKey = GlobalConn_DupName(name);
        Tcl_SetHashValue(he, gr);
    }
    else
    {
        gr = (GlobalConnRec*)Tcl_GetHashValue(he);
    }
    gr->ownerAlive = 1;
    atomic_store_explicit(&gr->pubConn, conn, memory_order_release);
    gr->refCount++;
    Tcl_MutexUnlock(&gConnMapMutex);
    return gr;
}

static GlobalConnRec* GlobalConn_LookupForAdoptAndRef(const char* name, dpiConn** outConn, int* pOwnerAlive)
{
    GlobalConnRec* gr = NULL;
    dpiConn* res = NULL;
    int ownerAlive = 0;

    Tcl_MutexLock(&gConnMapMutex);
    if (gConnMapInited)
    {
        Tcl_HashEntry* he = Tcl_FindHashEntry(&gConnByName, name);
        if (he)
        {
            gr = (GlobalConnRec*)Tcl_GetHashValue(he);
            ownerAlive = gr->ownerAlive;
            res = atomic_load_explicit(&gr->pubConn, memory_order_acquire);
            if (res && ownerAlive)
            {
                if (dpiConn_addRef(res) == DPI_SUCCESS)
                {
                    gr->refCount++;
                }
                else
                {
                    gr = NULL;
                    res = NULL;
                }
            }
            else
            {
                gr = NULL;
                res = NULL;
            }
        }
    }
    Tcl_MutexUnlock(&gConnMapMutex);

    if (outConn)
        *outConn = res;
    if (pOwnerAlive)
        *pOwnerAlive = ownerAlive;
    return gr;
}

static void GlobalConn_MarkOwnerGone(GlobalConnRec* gr)
{
    if (!gr)
        return;
    Tcl_MutexLock(&gConnMapMutex);
    gr->ownerAlive = 0;
    atomic_store_explicit(&gr->pubConn, NULL, memory_order_release);
    Tcl_MutexUnlock(&gConnMapMutex);
}

void Oradpi_SharedConnAddRef(GlobalConnRec* gr)
{
    if (!gr)
        return;
    Tcl_MutexLock(&gConnMapMutex);
    gr->refCount++;
    Tcl_MutexUnlock(&gConnMapMutex);
}

void Oradpi_SharedConnRelease(GlobalConnRec* gr)
{
    int doFree = 0;

    if (!gr)
        return;

    Tcl_MutexLock(&gConnMapMutex);
    if (gr->refCount > 0)
        gr->refCount--;
    if (gr->refCount == 0)
    {
        if (gConnMapInited && gr->nameKey)
        {
            Tcl_HashEntry* he = Tcl_FindHashEntry(&gConnByName, gr->nameKey);
            if (he && Tcl_GetHashValue(he) == gr)
                Tcl_DeleteHashEntry(he);
        }
        doFree = 1;
    }
    Tcl_MutexUnlock(&gConnMapMutex);

    if (doFree)
    {
        Tcl_ConditionFinalize(&gr->connCond);
        Tcl_MutexFinalize(&gr->connLock);
        if (gr->nameKey)
            Tcl_Free(gr->nameKey);
        Tcl_Free((char*)gr);
    }
}

static void GlobalConn_DeadlineFromNow(Tcl_Time* deadline, int timeoutMs)
{
    Tcl_GetTime(deadline);
    deadline->sec += timeoutMs / 1000;
    deadline->usec += (timeoutMs % 1000) * 1000;
    while (deadline->usec >= 1000000)
    {
        deadline->sec++;
        deadline->usec -= 1000000;
    }
}

int Oradpi_SharedConnGateEnterTimed(GlobalConnRec* gr, int timeoutMs)
{
    Tcl_ThreadId self;

    if (!gr)
        return 1;

    self = Tcl_GetCurrentThread();
    Tcl_MutexLock(&gr->connLock);

    if (gr->opDepth > 0 && gr->opOwner == self)
    {
        gr->opDepth++;
        Tcl_MutexUnlock(&gr->connLock);
        return 1;
    }

    if (timeoutMs >= 0)
    {
        Tcl_Time deadline;
        GlobalConn_DeadlineFromNow(&deadline, timeoutMs);
        while (gr->opDepth > 0)
        {
            Tcl_ConditionWait(&gr->connCond, &gr->connLock, &deadline);
            if (gr->opDepth == 0)
                break;
            Tcl_Time now;
            Tcl_GetTime(&now);
            if (now.sec > deadline.sec || (now.sec == deadline.sec && now.usec >= deadline.usec))
            {
                Tcl_MutexUnlock(&gr->connLock);
                return 0;
            }
        }
    }
    else
    {
        while (gr->opDepth > 0)
            Tcl_ConditionWait(&gr->connCond, &gr->connLock, NULL);
    }

    gr->opOwner = self;
    gr->opDepth = 1;
    Tcl_MutexUnlock(&gr->connLock);
    return 1;
}

void Oradpi_SharedConnGateEnter(GlobalConnRec* gr)
{
    (void)Oradpi_SharedConnGateEnterTimed(gr, -1);
}

void Oradpi_SharedConnGateLeave(GlobalConnRec* gr)
{
    if (!gr)
        return;

    Tcl_MutexLock(&gr->connLock);
    if (gr->opDepth > 0 && gr->opOwner == Tcl_GetCurrentThread())
    {
        gr->opDepth--;
        if (gr->opDepth == 0)
        {
            gr->opOwner = (Tcl_ThreadId)0;
            Tcl_ConditionNotify(&gr->connCond);
        }
    }
    Tcl_MutexUnlock(&gr->connLock);
}

void Oradpi_SharedConnBreak(GlobalConnRec* gr, dpiConn* conn)
{
    if (!gr || !conn)
        return;
    Tcl_MutexLock(&gr->connLock);
    Tcl_MutexUnlock(&gr->connLock);
    (void)dpiConn_breakExecution(conn);
}

void Oradpi_ConnGateEnter(OradpiConn* co)
{
    if (co)
        Oradpi_SharedConnGateEnter(co->shared);
}

int Oradpi_ConnGateEnterTimed(OradpiConn* co, int timeoutMs)
{
    return co ? Oradpi_SharedConnGateEnterTimed(co->shared, timeoutMs) : 1;
}

void Oradpi_ConnGateLeave(OradpiConn* co)
{
    if (co)
        Oradpi_SharedConnGateLeave(co->shared);
}

void Oradpi_ConnBreak(OradpiConn* co)
{
    if (co)
        Oradpi_SharedConnBreak(co->shared, co->conn);
}

void* Oradpi_ConnGateToken(OradpiConn* co)
{
    return co ? (void*)co->shared : NULL;
}

/* V-8 fix: Copy behavioral policy from an OradpiConn wrapper to its
 * GlobalConnRec snapshot.  Called at publish time and after oraconfig
 * changes, so that adopters in other interps inherit the owner's policy. */
void Oradpi_SharedConnSyncBehavior(OradpiConn* co)
{
    if (!co || !co->shared)
        return;
    GlobalConnRec* gr = co->shared;
    Tcl_MutexLock(&gConnMapMutex);
    gr->snap_autocommit = co->autocommit;
    gr->snap_fetchArraySize = co->fetchArraySize;
    gr->snap_prefetchRows = co->prefetchRows;
    gr->snap_inlineLobs = co->inlineLobs;
    gr->snap_foMaxAttempts = co->foMaxAttempts;
    gr->snap_foBackoffMs = co->foBackoffMs;
    gr->snap_foBackoffFactor = co->foBackoffFactor;
    gr->snap_foErrorClasses = co->foErrorClasses;
    gr->snap_foDebounceMs = co->foDebounceMs;
    Tcl_MutexUnlock(&gConnMapMutex);
}

static void Oradpi_RegisterConnInInterp(OradpiInterpState* st, OradpiConn* co)
{
    int newEntry;
    const char* hname = Tcl_GetString(co->base.name);
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->conns, hname, &newEntry);
    Tcl_SetHashValue(e, co);
    co->shared = GlobalConn_PublishAndRef(hname, co->conn);
    /* V-8 fix: Populate behavioral snapshot on the shared record so
     * adopters inherit the owner's policy instead of defaults. */
    Oradpi_SharedConnSyncBehavior(co);
}

/* ---- Centralized OradpiMsg cleanup ---- */
void Oradpi_FreeMsg(OradpiMsg* m)
{
    if (!m)
        return;
    if (m->fn)
    {
        Tcl_DecrRefCount(m->fn);
        m->fn = NULL;
    }
    if (m->sqlstate)
    {
        Tcl_DecrRefCount(m->sqlstate);
        m->sqlstate = NULL;
    }
    if (m->action)
    {
        Tcl_DecrRefCount(m->action);
        m->action = NULL;
    }
    if (m->error)
    {
        Tcl_DecrRefCount(m->error);
        m->error = NULL;
    }
}

void Oradpi_FreeConn(OradpiConn* co)
{
    GlobalConnRec* shared;

    if (!co)
        return;

    shared = co->shared;

    if (co->foTimerScheduled && co->foTimer)
    {
        Tcl_DeleteTimerHandler(co->foTimer);
        co->foTimer = NULL;
        co->foTimerScheduled = 0;
    }
    if (co->foPendingMsg)
    {
        Tcl_DecrRefCount(co->foPendingMsg);
        co->foPendingMsg = NULL;
    }
    if (co->failoverCallback)
    {
        Tcl_DecrRefCount(co->failoverCallback);
        co->failoverCallback = NULL;
    }

    if (co->conn)
    {
        if (co->ownerClose)
        {
            GlobalConn_MarkOwnerGone(shared); /* block further adoptions */
            /* Only close the server-side session if no adopters remain.
             * dpiConn_close() terminates the session on the server, which
             * would break any adopted handles in other interps that share
             * the same dpiConn* via addRef.  When adopters are still active
             * (refCount > 1 — the owner's own ref plus adopter refs), we
             * skip the close and let the last dpiConn_release() handle
             * cleanup automatically via ODPI-C reference counting. */
            int hasAdopters = 0;
            Tcl_MutexLock(&gConnMapMutex);
            hasAdopters = (shared->refCount > 1);
            Tcl_MutexUnlock(&gConnMapMutex);
            if (!hasAdopters)
            {
                if (Oradpi_SharedConnGateEnterTimed(shared, ORADPI_TEARDOWN_TIMEOUT_MS))
                {
                    dpiConn_close(co->conn, DPI_MODE_CONN_CLOSE_DEFAULT, NULL, 0);
                    Oradpi_SharedConnGateLeave(shared);
                }
            }
            /* else: adopters still active — skip close; or timed gate failed —
             * detach without close; worker will release its addRef'd handle. */
        }
        dpiConn_release(co->conn);
        co->conn = NULL;
    }
    if (co->pool)
    {
        dpiPool_close(co->pool, DPI_MODE_POOL_CLOSE_DEFAULT);
        dpiPool_release(co->pool);
        co->pool = NULL;
    }

    if (co->base.name)
    {
        Tcl_DecrRefCount(co->base.name);
        co->base.name = NULL;
    }
    Oradpi_FreeMsg(&co->base.msg);

    if (co->cachedEncoding)
    {
        Tcl_Free(co->cachedEncoding);
        co->cachedEncoding = NULL;
    }

    co->shared = NULL;
    Oradpi_SharedConnRelease(shared);
    Tcl_Free((char*)co);
}

void Oradpi_FreeStmt(Tcl_Interp* ip, OradpiStmt* s)
{
    if (!s)
        return;
    if (s->stmt)
    {
        /* M-4: Use a finite timeout (30s) instead of blocking indefinitely.
         * If the Oracle server is unreachable, dpiConn_breakExecution may
         * not unblock the worker thread, causing interp teardown to hang.
         * On timeout, we proceed with cleanup; the worker's own addRef'd
         * handles will be released when it eventually returns. */
        (void)Oradpi_StmtWaitForAsync(s, 1 /*cancel*/, 30000 /*30s timeout*/);
        /* Use timed gate to avoid infinite hang if a worker still holds the
         * gate (e.g. breakExecution failed to interrupt a stuck OCI call).
         * On timeout: detach wrapper without calling dpiStmt_close/release —
         * the worker holds its own addRef'd handles and will clean up when
         * it eventually returns. */
        if (s->owner && CONN_GATE_ENTER_TIMED(s->owner, ORADPI_TEARDOWN_TIMEOUT_MS))
        {
            dpiStmt_close(s->stmt, NULL, 0);
            dpiStmt_release(s->stmt);
            CONN_GATE_LEAVE(s->owner);
        }
        else if (!s->owner)
        {
            /* No owner connection (orphan stmt) — close ungated */
            dpiStmt_close(s->stmt, NULL, 0);
            dpiStmt_release(s->stmt);
        }
        /* else: timed gate failed — detach; worker will release its addRef'd handle */
        s->stmt = NULL;
    }
    s->owner = NULL;
    /* Clean up bind stores and pending refs for this statement */
    if (ip && s->base.name)
    {
        const char* skey = Tcl_GetString(s->base.name);
        Oradpi_BindStoreForget(ip, skey);
        Oradpi_PendingsForget(ip, skey);
    }
    if (s->base.name)
    {
        Tcl_DecrRefCount(s->base.name);
        s->base.name = NULL;
    }
    Oradpi_FreeMsg(&s->base.msg);
    Tcl_Free((char*)s);
}

/* Remove statement from interp hash table and free it */
void Oradpi_RemoveStmt(Tcl_Interp* ip, OradpiStmt* s)
{
    if (!ip || !s)
        return;
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st && s->base.name)
    {
        const char* hname = Tcl_GetString(s->base.name);
        Tcl_HashEntry* e = Tcl_FindHashEntry(&st->stmts, hname);
        if (e)
            Tcl_DeleteHashEntry(e);
    }
    Oradpi_FreeStmt(ip, s);
}

/* V-8 fix: Remove a LOB from interp state and free it.  Mirrors
 * Oradpi_RemoveStmt() — needed for oralogoff LOB cleanup. */
void Oradpi_RemoveLob(Tcl_Interp* ip, OradpiLob* l)
{
    if (!ip || !l)
        return;
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st && l->base.name)
    {
        const char* hname = Tcl_GetString(l->base.name);
        Tcl_HashEntry* e = Tcl_FindHashEntry(&st->lobs, hname);
        if (e)
            Tcl_DeleteHashEntry(e);
    }
    Oradpi_FreeLob(l);
}

void Oradpi_FreeLob(OradpiLob* l)
{
    if (!l)
        return;
    GlobalConnRec* shared = l->shared;
    if (l->lob)
    {
        /* V-8 fix: Use bounded timed gate instead of unconditional gate.
         * Statement and connection teardown already use bounded waits to
         * avoid hanging interp destruction.  LOB teardown was the only path
         * that could block indefinitely.  On timeout, detach without closing —
         * the stuck worker will eventually release its own addRef'd handle. */
        if (shared && Oradpi_SharedConnGateEnterTimed(shared, ORADPI_TEARDOWN_TIMEOUT_MS))
        {
            dpiLob_close(l->lob);
            dpiLob_release(l->lob);
            Oradpi_SharedConnGateLeave(shared);
        }
        else if (!shared)
        {
            /* No shared gate (orphan LOB) — close ungated */
            dpiLob_close(l->lob);
            dpiLob_release(l->lob);
        }
        /* else: timed gate failed — detach; worker will release its handle */
        l->lob = NULL;
    }
    if (l->base.name)
    {
        Tcl_DecrRefCount(l->base.name);
        l->base.name = NULL;
    }
    Oradpi_FreeMsg(&l->base.msg);
    l->shared = NULL;
    Oradpi_SharedConnRelease(shared);
    Tcl_Free((char*)l);
}

static OradpiInterpState* Oradpi_Get(Tcl_Interp* ip)
{
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st)
        return st;
    st = (OradpiInterpState*)Tcl_Alloc(sizeof(*st));
    memset(st, 0, sizeof(*st));
    st->ip = ip;
    Tcl_InitHashTable(&st->conns, TCL_STRING_KEYS);
    Tcl_InitHashTable(&st->stmts, TCL_STRING_KEYS);
    Tcl_InitHashTable(&st->lobs, TCL_STRING_KEYS);
    Tcl_SetAssocData(ip, "oradpi", Oradpi_DeleteInterpData, st);
    return st;
}

void Oradpi_DeleteInterpData(void* clientData, Tcl_Interp* ip)
{
    OradpiInterpState* st = (OradpiInterpState*)clientData;
    if (!st)
        return;
    Tcl_HashSearch search;
    Tcl_HashEntry* e;

    /* Phase 1: Cancel all async operations for all connections.
     * This must happen before freeing statements or connections
     * so async worker threads don't access released handles. */
    for (e = Tcl_FirstHashEntry(&st->conns, &search); e; e = Tcl_NextHashEntry(&search))
    {
        OradpiConn* co = (OradpiConn*)Tcl_GetHashValue(e);
        if (co)
            Oradpi_CancelAndJoinAllForConn(ip, co);
    }

    /* Phase 2: Free LOBs */
    for (e = Tcl_FirstHashEntry(&st->lobs, &search); e; e = Tcl_NextHashEntry(&search))
        Oradpi_FreeLob((OradpiLob*)Tcl_GetHashValue(e));
    Tcl_DeleteHashTable(&st->lobs);

    /* Phase 3: Free statements (async already cancelled in phase 1) */
    for (e = Tcl_FirstHashEntry(&st->stmts, &search); e; e = Tcl_NextHashEntry(&search))
        Oradpi_FreeStmt(ip, (OradpiStmt*)Tcl_GetHashValue(e));
    Tcl_DeleteHashTable(&st->stmts);

    /* Phase 4: Free connections */
    for (e = Tcl_FirstHashEntry(&st->conns, &search); e; e = Tcl_NextHashEntry(&search))
        Oradpi_FreeConn((OradpiConn*)Tcl_GetHashValue(e));
    Tcl_DeleteHashTable(&st->conns);

    Tcl_Free((char*)st);
}

OradpiConn* Oradpi_NewConn(Tcl_Interp* ip, dpiConn* conn, dpiPool* pool)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    OradpiConn* co = (OradpiConn*)Tcl_Alloc(sizeof(*co));
    memset(co, 0, sizeof(*co));
    co->base.name = Oradpi_NewHandleName(ip, "oraL");
    Tcl_IncrRefCount(co->base.name);
    co->conn = conn;
    co->pool = pool;
    co->autocommit = 0;
    co->fetchArraySize = DPI_DEFAULT_FETCH_ARRAY_SIZE;
    co->prefetchRows = DPI_DEFAULT_PREFETCH_ROWS;
    co->callTimeout = 0;
    co->inlineLobs = 0;
    co->stmtCacheSize = 0;
    co->ownerClose = 1;
    co->cachedEncoding = NULL;
    if (co->conn)
    {
        uint32_t v = 0;
        if (dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
            co->stmtCacheSize = v;
        if (dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
            co->callTimeout = v;
        /* Cache encoding once at connection time (perf fix 4.2) */
        dpiEncodingInfo enc;
        memset(&enc, 0, sizeof enc);
        if (dpiConn_getEncodingInfo(co->conn, &enc) == DPI_SUCCESS && enc.encoding)
        {
            Tcl_Size elen = (Tcl_Size)strlen(enc.encoding);
            size_t encBytes = 0;
            if (Oradpi_CheckedAllocBytes(NULL, elen + 1, sizeof(char), &encBytes, "connection encoding cache") == TCL_OK)
            {
                co->cachedEncoding = (char*)Tcl_Alloc(encBytes);
                memcpy(co->cachedEncoding, enc.encoding, encBytes);
            }
        }
    }
    Oradpi_RegisterConnInInterp(st, co);
    return co;
}

static OradpiConn* Oradpi_AdoptConn(Tcl_Interp* ip, const char* handleName, GlobalConnRec* shared, dpiConn* connFromOwner)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    OradpiConn* co = (OradpiConn*)Tcl_Alloc(sizeof(*co));
    memset(co, 0, sizeof(*co));

    co->base.name = Tcl_NewStringObj(handleName, -1);
    Tcl_IncrRefCount(co->base.name);
    /* GlobalConn_LookupForAdoptAndRef already performed dpiConn_addRef() and
     * incremented the shared record refcount under gConnMapMutex. */
    co->conn = connFromOwner;
    co->shared = shared;
    co->pool = NULL;

    /* V-8 fix: Copy behavioral policy from the shared snapshot instead of
     * using hardcoded defaults.  This ensures the same dpiConn* behaves
     * consistently across interps.  The snapshot is populated by the owner
     * at publish time and updated by oraconfig. */
    Tcl_MutexLock(&gConnMapMutex);
    co->autocommit = shared->snap_autocommit;
    co->fetchArraySize = shared->snap_fetchArraySize;
    co->prefetchRows = shared->snap_prefetchRows;
    co->inlineLobs = shared->snap_inlineLobs;
    co->foMaxAttempts = shared->snap_foMaxAttempts;
    co->foBackoffMs = shared->snap_foBackoffMs;
    co->foBackoffFactor = shared->snap_foBackoffFactor;
    co->foErrorClasses = shared->snap_foErrorClasses;
    co->foDebounceMs = shared->snap_foDebounceMs;
    Tcl_MutexUnlock(&gConnMapMutex);

    co->callTimeout = 0;
    co->stmtCacheSize = 0;
    co->ownerClose = 0;
    co->adopted = 1;
    co->cachedEncoding = NULL;
    if (co->conn)
    {
        uint32_t v = 0;
        CONN_GATE_ENTER(co);
        if (dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
            co->callTimeout = v;
        if (dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
            co->stmtCacheSize = v;
        dpiEncodingInfo enc;
        memset(&enc, 0, sizeof enc);
        if (dpiConn_getEncodingInfo(co->conn, &enc) == DPI_SUCCESS && enc.encoding)
        {
            Tcl_Size elen = (Tcl_Size)strlen(enc.encoding);
            size_t encBytes = 0;
            if (Oradpi_CheckedAllocBytes(NULL, elen + 1, sizeof(char), &encBytes, "adopted connection encoding cache") == TCL_OK)
            {
                co->cachedEncoding = (char*)Tcl_Alloc(encBytes);
                memcpy(co->cachedEncoding, enc.encoding, encBytes);
            }
        }
        CONN_GATE_LEAVE(co);
    }

    int newEntry;
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->conns, handleName, &newEntry);
    Tcl_SetHashValue(e, co);
    return co;
}

OradpiConn* Oradpi_LookupConn(Tcl_Interp* ip, Tcl_Obj* nameObj)
{
    if (!ip || !nameObj)
        return NULL;
    OradpiInterpState* st = Oradpi_Get(ip);
    const char* hname = Tcl_GetString(nameObj);
    Tcl_HashEntry* e = Tcl_FindHashEntry(&st->conns, hname);

    if (e)
    {
        OradpiConn* co = (OradpiConn*)Tcl_GetHashValue(e);
        if (co && co->conn)
            return co;
        /* Dead connection: remove from hash and free the struct (fix 2.3) */
        Tcl_DeleteHashEntry(e);
        if (co)
            Oradpi_FreeConn(co);
    }

    int ownerAlive = 0;
    dpiConn* sharedConn = NULL;
    GlobalConnRec* shared = GlobalConn_LookupForAdoptAndRef(hname, &sharedConn, &ownerAlive);
    if (shared && ownerAlive && sharedConn)
        return Oradpi_AdoptConn(ip, hname, shared, sharedConn);

    return NULL;
}

OradpiStmt* Oradpi_NewStmt(Tcl_Interp* ip, OradpiConn* co)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    OradpiStmt* s = (OradpiStmt*)Tcl_Alloc(sizeof(*s));
    memset(s, 0, sizeof(*s));
    s->base.name = Oradpi_NewHandleName(ip, "oraS");
    Tcl_IncrRefCount(s->base.name);
    s->owner = co;
    /* V-8 fix: Inherit connection-level fetchArraySize so that the
     * parse-time dpiStmt_setFetchArraySize() call at cmd_stmt.c fires,
     * and so the statement-level getter reports the effective value. */
    if (co)
        s->fetchArray = co->fetchArraySize;
    int newEntry;
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->stmts, Tcl_GetString(s->base.name), &newEntry);
    Tcl_SetHashValue(e, s);
    return s;
}

OradpiStmt* Oradpi_LookupStmt(Tcl_Interp* ip, Tcl_Obj* nameObj)
{
    if (!ip || !nameObj)
        return NULL;
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (!st)
        return NULL;
    const char* name = Tcl_GetString(nameObj);
    Tcl_HashEntry* e = Tcl_FindHashEntry(&st->stmts, name);
    return e ? (OradpiStmt*)Tcl_GetHashValue(e) : NULL;
}

OradpiLob* Oradpi_NewLob(Tcl_Interp* ip, dpiLob* lob, GlobalConnRec* shared)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    OradpiLob* l = (OradpiLob*)Tcl_Alloc(sizeof(*l));
    memset(l, 0, sizeof(*l));
    l->base.name = Oradpi_NewHandleName(ip, "oraB");
    Tcl_IncrRefCount(l->base.name);
    l->lob = lob;
    l->shared = shared;
    Oradpi_SharedConnAddRef(shared);
    int newEntry;
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->lobs, Tcl_GetString(l->base.name), &newEntry);
    Tcl_SetHashValue(e, l);
    return l;
}

OradpiLob* Oradpi_LookupLob(Tcl_Interp* ip, Tcl_Obj* nameObj)
{
    if (!ip || !nameObj)
        return NULL;
    OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (!st)
        return NULL;
    const char* name = Tcl_GetString(nameObj);
    Tcl_HashEntry* e = Tcl_FindHashEntry(&st->lobs, name);
    return e ? (OradpiLob*)Tcl_GetHashValue(e) : NULL;
}
