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

#include <string.h>

#include "cmd_int.h"

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static void GlobalConn_Erase(const char* name);
static dpiConn* GlobalConn_Lookup(const char* name, int* pOwnerAlive);
static void GlobalConn_MarkOwnerGone(const char* name);
static void GlobalConn_Publish(const char* name, dpiConn* conn);
static void GlobalConnMap_Init(void);
static OradpiConn* Oradpi_AdoptConn(Tcl_Interp* ip, const char* handleName, dpiConn* connFromOwner);
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
OradpiLob* Oradpi_NewLob(Tcl_Interp* ip, dpiLob* lob);
OradpiStmt* Oradpi_NewStmt(Tcl_Interp* ip, OradpiConn* co);
static void Oradpi_RegisterConnInInterp(OradpiInterpState* st, OradpiConn* co);

/* ------------------------------------------------------------------------- *
 * Stuff
 * ------------------------------------------------------------------------- */

typedef struct GlobalConnRec
{
    dpiConn* conn;
    int ownerAlive; /* 1 while creator interp hasn't destroyed its OradpiConn/logged off */
} GlobalConnRec;

/* gConnMapMutex: protects gConnByName hash table and gConnMapInited flag.
 * Lock ordering: leaf lock, no other module locks held while this is held. */
static Tcl_Mutex gConnMapMutex;
static int gConnMapInited = 0;
static Tcl_HashTable gConnByName;

static void GlobalConnMap_Init(void)
{
    Tcl_MutexLock(&gConnMapMutex);
    if (!gConnMapInited)
    {
        Tcl_InitHashTable(&gConnByName, TCL_STRING_KEYS);
        gConnMapInited = 1;
    }
    Tcl_MutexUnlock(&gConnMapMutex);
}

static void GlobalConn_Publish(const char* name, dpiConn* conn)
{
    GlobalConnMap_Init();
    Tcl_MutexLock(&gConnMapMutex);
    int newEntry = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&gConnByName, name, &newEntry);
    if (newEntry)
    {
        GlobalConnRec* gr = (GlobalConnRec*)Tcl_Alloc(sizeof(*gr));
        gr->conn = conn;
        gr->ownerAlive = 1;
        Tcl_SetHashValue(he, gr);
    }
    else
    {
        GlobalConnRec* gr = (GlobalConnRec*)Tcl_GetHashValue(he);
        gr->conn = conn;
        gr->ownerAlive = 1;
    }
    Tcl_MutexUnlock(&gConnMapMutex);
}

static dpiConn* GlobalConn_Lookup(const char* name, int* pOwnerAlive)
{
    dpiConn* res = NULL;
    int ownerAlive = 0;
    Tcl_MutexLock(&gConnMapMutex);
    if (!gConnMapInited)
    {
        Tcl_MutexUnlock(&gConnMapMutex);
        if (pOwnerAlive)
            *pOwnerAlive = 0;
        return NULL;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gConnByName, name);
    if (he)
    {
        GlobalConnRec* gr = (GlobalConnRec*)Tcl_GetHashValue(he);
        res = gr->conn;
        ownerAlive = gr->ownerAlive;
        /* addRef while still holding the lock so the owner can't free
         * the conn between our return and the caller's use (fix 5.2) */
        if (res && ownerAlive)
            dpiConn_addRef(res);
    }
    Tcl_MutexUnlock(&gConnMapMutex);
    if (pOwnerAlive)
        *pOwnerAlive = ownerAlive;
    return res;
}

static void GlobalConn_MarkOwnerGone(const char* name)
{
    Tcl_MutexLock(&gConnMapMutex);
    if (!gConnMapInited)
    {
        Tcl_MutexUnlock(&gConnMapMutex);
        return;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gConnByName, name);
    if (he)
    {
        GlobalConnRec* gr = (GlobalConnRec*)Tcl_GetHashValue(he);
        gr->ownerAlive = 0;
    }
    Tcl_MutexUnlock(&gConnMapMutex);
}

static void GlobalConn_Erase(const char* name)
{
    Tcl_MutexLock(&gConnMapMutex);
    if (!gConnMapInited)
    {
        Tcl_MutexUnlock(&gConnMapMutex);
        return;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gConnByName, name);
    if (he)
    {
        GlobalConnRec* gr = (GlobalConnRec*)Tcl_GetHashValue(he);
        Tcl_Free((char*)gr);
        Tcl_DeleteHashEntry(he);
    }
    Tcl_MutexUnlock(&gConnMapMutex);
}

static void Oradpi_RegisterConnInInterp(OradpiInterpState* st, OradpiConn* co)
{
    int newEntry;
    const char* hname = Tcl_GetString(co->base.name);
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->conns, hname, &newEntry);
    Tcl_SetHashValue(e, co);
    GlobalConn_Publish(hname, co->conn);
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
    if (!co)
        return;

    const char* hname = (co->base.name ? Tcl_GetString(co->base.name) : NULL);

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
            if (hname)
                GlobalConn_MarkOwnerGone(hname); /* block further adoptions */
            dpiConn_close(co->conn, DPI_MODE_CONN_CLOSE_DEFAULT, NULL, 0);
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

    if (co->ownerClose && hname)
    {
        GlobalConn_Erase(hname);
    }

    /* Free base.name AFTER GlobalConn_Erase since hname points into it (fix 2.5) */
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

    Tcl_Free((char*)co);
}

void Oradpi_FreeStmt(Tcl_Interp* ip, OradpiStmt* s)
{
    if (!s)
        return;
    if (s->stmt)
    {
        (void)Oradpi_StmtWaitForAsync(s, 1 /*cancel*/, -1 /*wait indefinitely*/);
        dpiStmt_close(s->stmt, NULL, 0);
        dpiStmt_release(s->stmt);
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

void Oradpi_FreeLob(OradpiLob* l)
{
    if (!l)
        return;
    if (l->lob)
    {
        dpiLob_close(l->lob);
        dpiLob_release(l->lob);
        l->lob = NULL;
    }
    if (l->base.name)
    {
        Tcl_DecrRefCount(l->base.name);
        l->base.name = NULL;
    }
    Oradpi_FreeMsg(&l->base.msg);
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
    co->prefetchMemory = 0;
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
            size_t elen = strlen(enc.encoding);
            co->cachedEncoding = (char*)Tcl_Alloc(elen + 1);
            memcpy(co->cachedEncoding, enc.encoding, elen + 1);
        }
    }
    Oradpi_RegisterConnInInterp(st, co);
    return co;
}

static OradpiConn* Oradpi_AdoptConn(Tcl_Interp* ip, const char* handleName, dpiConn* connFromOwner)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    OradpiConn* co = (OradpiConn*)Tcl_Alloc(sizeof(*co));
    memset(co, 0, sizeof(*co));

    co->base.name = Tcl_NewStringObj(handleName, -1);
    Tcl_IncrRefCount(co->base.name);
    /* Note: GlobalConn_Lookup already called dpiConn_addRef under lock (fix 5.2),
     * so we just take ownership of that reference here. */
    co->conn = connFromOwner;
    co->pool = NULL;
    co->autocommit = 0;
    co->fetchArraySize = DPI_DEFAULT_FETCH_ARRAY_SIZE;
    co->prefetchRows = DPI_DEFAULT_PREFETCH_ROWS;
    co->prefetchMemory = 0;
    co->callTimeout = 0;
    co->inlineLobs = 0;
    co->stmtCacheSize = 0;
    co->ownerClose = 0;
    co->cachedEncoding = NULL;

    int newEntry;
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->conns, handleName, &newEntry);
    Tcl_SetHashValue(e, co);
    return co;
}

OradpiConn* Oradpi_LookupConn(Tcl_Interp* ip, Tcl_Obj* nameObj)
{
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
    dpiConn* shared = GlobalConn_Lookup(hname, &ownerAlive);
    if (shared && ownerAlive)
    {
        return Oradpi_AdoptConn(ip, hname, shared);
    }

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
    int newEntry;
    Tcl_HashEntry* e = Tcl_CreateHashEntry(&st->stmts, Tcl_GetString(s->base.name), &newEntry);
    Tcl_SetHashValue(e, s);
    return s;
}

OradpiStmt* Oradpi_LookupStmt(Tcl_Interp* ip, Tcl_Obj* nameObj)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    Tcl_HashEntry* e = Tcl_FindHashEntry(&st->stmts, Tcl_GetString(nameObj));
    return e ? (OradpiStmt*)Tcl_GetHashValue(e) : NULL;
}

OradpiLob* Oradpi_NewLob(Tcl_Interp* ip, dpiLob* lob)
{
    OradpiInterpState* st = Oradpi_Get(ip);
    OradpiLob* l = (OradpiLob*)Tcl_Alloc(sizeof(*l));
    memset(l, 0, sizeof(*l));
    l->base.name = Oradpi_NewHandleName(ip, "oraB");
    Tcl_IncrRefCount(l->base.name);
    l->lob = lob;
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
