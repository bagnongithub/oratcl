/*
 *  async.c --
 *
 *    Asynchronous execution support with thread pool.
 *
 *        - Maintains a lazily-initialized pool of persistent worker threads
 *          with a shared work queue, replacing the old thread-per-op design.
 *        - Tracks per-statement async state; provides start/cancel/join and
 *          safe teardown semantics.
 *        - Registry protected by Tcl mutexes; keys are internal pointers to
 *          avoid cross-interp naming clashes.
 *        - Uses Tcl_Condition for signaling completion.
 *        - Pool is created on first oraexecasync, torn down at process exit.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <tcl.h>

#include "cmd_int.h"
#include "dpi.h"

/* =========================================================================
 * Per-statement async entry
 * ========================================================================= */

typedef struct OradpiAsyncEntry
{
    Tcl_ThreadId tid;   /* pool thread that picked up the job (diagnostic) */
    Tcl_Mutex lock;     /* protects all mutable fields in this struct */
    Tcl_Condition cond; /* signaled when done is set */
    int running;
    int done;
    int canceled;
    int joined;
    int rc;
    int errorCode;
    char* errorMsg;

    dpiConn* conn;
    dpiStmt* stmt;
    OradpiConn* owner;

    int doCommit;
    int autocommit;

    /* Snapshotted failover policy (copied at enqueue time to avoid data races) */
    uint32_t foMaxAttempts;
    uint32_t foBackoffMs;
    double foBackoffFactor;
    uint32_t foErrorClasses;

    char* stmtKey;
} OradpiAsyncEntry;

/* =========================================================================
 * Thread-pool work queue
 * ========================================================================= */

typedef struct PoolWorkItem
{
    OradpiStmt* s;
    struct PoolWorkItem* next;
} PoolWorkItem;

typedef struct OradpiThreadPool
{
    Tcl_Mutex queueMutex;
    Tcl_Condition queueCond;
    PoolWorkItem* head;
    PoolWorkItem* tail;

    Tcl_ThreadId* threads;
    int nThreads;
    int shutdown;
    int started;
} OradpiThreadPool;

#define ORADPI_DEFAULT_POOL_SIZE 4

/* =========================================================================
 * Forward Declarations
 * ========================================================================= */

static OradpiAsyncEntry* AsyncEnsure(OradpiStmt* s, int* isNewOut);
static OradpiAsyncEntry* AsyncLookup(OradpiStmt* s);
static void AsyncRegistryEnsure(void);
static void AsyncRemove(OradpiStmt* s);
static void AsyncWorkerBody(OradpiStmt* s);
void Oradpi_CancelAndJoinAllForConn(Tcl_Interp* ip, OradpiConn* co);
int Oradpi_Cmd_ExecAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_WaitAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_StmtWaitForAsync(OradpiStmt* s, int cancel, int timeoutMs);
int Oradpi_StmtIsAsyncBusy(OradpiStmt* s);

static void PoolEnsure(void);
static void PoolEnqueue(OradpiStmt* s);
static void PoolExitHandler(void* unused);
static Tcl_ThreadCreateProc PoolThreadProc;

/* Named constants for failover error-class bitmasks (must match cmd_exec.c) */
#define ORADPI_FO_CLASS_NETWORK 0x01
#define ORADPI_FO_CLASS_CONNLOST 0x02
#define ORA_ERR_BROKEN_PIPE 3113
#define ORA_ERR_NOT_CONNECTED 3114
#define ORA_ERR_LOST_CONTACT 3135
#define ORA_ERR_TNS_LOST_CONTACT 12571

extern dpiContext* Oradpi_GlobalDpiContext;
void Oradpi_PendingsForget(Tcl_Interp* ip, const char* stmtKey);

/* =========================================================================
 * Globals
 * ========================================================================= */

static Tcl_HashTable gAsyncByStmt;
static int gAsyncInit = 0;
/* gAsyncMutex: protects gAsyncByStmt and gAsyncInit.
 * Lock ordering: acquire before any per-entry ae->lock. */
static Tcl_Mutex gAsyncMutex;

static OradpiThreadPool gPool;
static int gPoolInited = 0;
/* gPoolInitMutex: protects gPool struct and gPoolInited flag.
 * Lock ordering: leaf lock (no other locks held while this is held). */
static Tcl_Mutex gPoolInitMutex;

/* =========================================================================
 * Async registry
 * ========================================================================= */

static void AsyncRegistryEnsure(void)
{
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_InitHashTable(&gAsyncByStmt, TCL_ONE_WORD_KEYS);
        gAsyncInit = 1;
    }
    Tcl_MutexUnlock(&gAsyncMutex);
}

static OradpiAsyncEntry* AsyncLookup(OradpiStmt* s)
{
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_MutexUnlock(&gAsyncMutex);
        return NULL;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gAsyncByStmt, (const void*)s);
    OradpiAsyncEntry* ae = he ? (OradpiAsyncEntry*)Tcl_GetHashValue(he) : NULL;
    Tcl_MutexUnlock(&gAsyncMutex);
    return ae;
}

static OradpiAsyncEntry* AsyncEnsure(OradpiStmt* s, int* isNewOut)
{
    AsyncRegistryEnsure();
    Tcl_MutexLock(&gAsyncMutex);
    int isNew = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&gAsyncByStmt, (const void*)s, &isNew);
    OradpiAsyncEntry* ae;
    if (isNew)
    {
        ae = (OradpiAsyncEntry*)Tcl_Alloc(sizeof(*ae));
        memset(ae, 0, sizeof(*ae));
        Tcl_SetHashValue(he, ae);
    }
    else
    {
        ae = (OradpiAsyncEntry*)Tcl_GetHashValue(he);
    }
    Tcl_MutexUnlock(&gAsyncMutex);
    if (isNewOut)
        *isNewOut = isNew;
    return ae;
}

static void AsyncRemove(OradpiStmt* s)
{
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_MutexUnlock(&gAsyncMutex);
        return;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gAsyncByStmt, (const void*)s);
    if (he)
    {
        OradpiAsyncEntry* ae = (OradpiAsyncEntry*)Tcl_GetHashValue(he);
        if (ae)
        {
            if (ae->errorMsg)
                Tcl_Free(ae->errorMsg);
            if (ae->stmt)
                dpiStmt_release(ae->stmt);
            if (ae->conn)
                dpiConn_release(ae->conn);
            if (ae->stmtKey)
                Tcl_Free(ae->stmtKey);
            Tcl_ConditionFinalize(&ae->cond);
            Tcl_MutexFinalize(&ae->lock);
            Tcl_Free((char*)ae);
        }
        Tcl_DeleteHashEntry(he);
    }
    Tcl_MutexUnlock(&gAsyncMutex);
}

/* =========================================================================
 * Thread pool
 * ========================================================================= */

static void PoolThreadProc(void* cd)
{
    (void)cd;
    for (;;)
    {
        Tcl_MutexLock(&gPool.queueMutex);
        while (!gPool.head && !gPool.shutdown)
            Tcl_ConditionWait(&gPool.queueCond, &gPool.queueMutex, NULL);

        if (gPool.shutdown && !gPool.head)
        {
            Tcl_MutexUnlock(&gPool.queueMutex);
            break;
        }

        PoolWorkItem* item = gPool.head;
        gPool.head = item->next;
        if (!gPool.head)
            gPool.tail = NULL;
        Tcl_MutexUnlock(&gPool.queueMutex);

        OradpiStmt* s = item->s;
        Tcl_Free((char*)item);
        AsyncWorkerBody(s);
    }
}

static void PoolEnsure(void)
{
    Tcl_MutexLock(&gPoolInitMutex);
    if (gPoolInited)
    {
        Tcl_MutexUnlock(&gPoolInitMutex);
        return;
    }
    memset(&gPool, 0, sizeof(gPool));
    gPool.nThreads = ORADPI_DEFAULT_POOL_SIZE;
    gPool.threads = (Tcl_ThreadId*)Tcl_Alloc(sizeof(Tcl_ThreadId) * gPool.nThreads);
    memset(gPool.threads, 0, sizeof(Tcl_ThreadId) * gPool.nThreads);

    for (int i = 0; i < gPool.nThreads; i++)
    {
        if (Tcl_CreateThread(&gPool.threads[i], PoolThreadProc, NULL, TCL_THREAD_STACK_DEFAULT, TCL_THREAD_NOFLAGS) != TCL_OK)
        {
            gPool.nThreads = i;
            break;
        }
    }
    gPool.started = 1;
    gPoolInited = 1;
    Tcl_CreateExitHandler(PoolExitHandler, NULL);
    Tcl_MutexUnlock(&gPoolInitMutex);
}

static void PoolEnqueue(OradpiStmt* s)
{
    PoolWorkItem* item = (PoolWorkItem*)Tcl_Alloc(sizeof(*item));
    item->s = s;
    item->next = NULL;

    Tcl_MutexLock(&gPool.queueMutex);
    if (gPool.tail)
        gPool.tail->next = item;
    else
        gPool.head = item;
    gPool.tail = item;
    Tcl_ConditionNotify(&gPool.queueCond);
    Tcl_MutexUnlock(&gPool.queueMutex);
}

static void PoolExitHandler(void* unused)
{
    (void)unused;
    if (!gPoolInited)
        return;

    Tcl_MutexLock(&gPool.queueMutex);
    gPool.shutdown = 1;
    for (int i = 0; i < gPool.nThreads; i++)
        Tcl_ConditionNotify(&gPool.queueCond);
    Tcl_MutexUnlock(&gPool.queueMutex);

    for (int i = 0; i < gPool.nThreads; i++)
    {
        if (gPool.threads[i])
            (void)Tcl_JoinThread(gPool.threads[i], NULL);
    }

    Tcl_MutexLock(&gPool.queueMutex);
    while (gPool.head)
    {
        PoolWorkItem* item = gPool.head;
        gPool.head = item->next;
        Tcl_Free((char*)item);
    }
    Tcl_MutexUnlock(&gPool.queueMutex);

    Tcl_ConditionFinalize(&gPool.queueCond);
    Tcl_MutexFinalize(&gPool.queueMutex);
    if (gPool.threads)
    {
        Tcl_Free((char*)gPool.threads);
        gPool.threads = NULL;
    }
    gPoolInited = 0;
}

/* =========================================================================
 * Worker body (runs on pool thread)
 * ========================================================================= */

static void AsyncWorkerBody(OradpiStmt* s)
{
    OradpiAsyncEntry* ae = AsyncLookup(s);
    if (!ae)
        return;

    Tcl_MutexLock(&ae->lock);
    ae->tid = Tcl_GetCurrentThread();
    dpiStmt* stmt = ae->stmt;
    int doCommit = ae->doCommit;
    int autocommit = ae->autocommit;
    /* Use snapshotted failover policy (copied at enqueue time) to avoid
     * data races with the interpreter thread modifying oraconfig. */
    uint32_t maxAttempts = ae->foMaxAttempts;
    uint32_t backoffMs = ae->foBackoffMs;
    double backoffFact = ae->foBackoffFactor;
    uint32_t errClasses = ae->foErrorClasses;
    Tcl_MutexUnlock(&ae->lock);

    dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
    dpiStmtInfo info;
    if (dpiStmt_getInfo(stmt, &info) == DPI_SUCCESS)
    {
        if (doCommit || (autocommit && (info.isDML || info.isPLSQL)))
            mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;
    }

    /* Execute with retry/backoff if failover policy is configured (SUG-4).
     * maxAttempts/backoffMs/backoffFact/errClasses are already snapshotted above. */

    int execRc = DPI_FAILURE;
    uint32_t nqc = 0;
    dpiErrorInfo lastEi;
    memset(&lastEi, 0, sizeof(lastEi));

    uint32_t attempt = 0;
    uint32_t totalTries = (maxAttempts > 0 && errClasses) ? maxAttempts + 1 : 1;

    for (attempt = 0; attempt < totalTries; attempt++)
    {
        /* Check if canceled between retries */
        Tcl_MutexLock(&ae->lock);
        int wasCanceled = ae->canceled;
        Tcl_MutexUnlock(&ae->lock);
        if (wasCanceled)
            break;

        nqc = 0;
        execRc = dpiStmt_execute(stmt, mode, &nqc);
        if (execRc == DPI_SUCCESS)
            break;

        /* Capture error immediately (thread-local) */
        memset(&lastEi, 0, sizeof(lastEi));
        if (Oradpi_GlobalDpiContext)
            dpiContext_getError(Oradpi_GlobalDpiContext, &lastEi);

        /* Check if error matches configured retry classes */
        if (attempt + 1 < totalTries && errClasses)
        {
            int shouldRetry = 0;
            if ((errClasses & ORADPI_FO_CLASS_NETWORK) && lastEi.isRecoverable)
                shouldRetry = 1; /* network class */
            if ((errClasses & ORADPI_FO_CLASS_CONNLOST))
            {
                /* connlost class: ORA-03113, ORA-03114, ORA-03135, ORA-12571 */
                int c = (int)lastEi.code;
                if (c == ORA_ERR_BROKEN_PIPE || c == ORA_ERR_NOT_CONNECTED || c == ORA_ERR_LOST_CONTACT ||
                    c == ORA_ERR_TNS_LOST_CONTACT)
                    shouldRetry = 1;
            }
            if (!shouldRetry)
                break; /* non-retryable error; stop immediately */

            /* Exponential backoff sleep */
            double sleepMs = (double)backoffMs;
            for (uint32_t k = 0; k < attempt; k++)
                sleepMs *= backoffFact;
            if (sleepMs > 60000.0)
                sleepMs = 60000.0; /* cap at 60 seconds */
            Tcl_Sleep((int)sleepMs);
        }
    }

    if (execRc != DPI_SUCCESS)
    {
        /* If we didn't capture the error yet (single attempt path) */
        if (lastEi.message == NULL && Oradpi_GlobalDpiContext)
            dpiContext_getError(Oradpi_GlobalDpiContext, &lastEi);

        Tcl_MutexLock(&ae->lock);
        ae->rc = -1;
        ae->errorCode = (int)lastEi.code;
        if (lastEi.message && lastEi.messageLength > 0)
        {
            size_t L = (size_t)lastEi.messageLength;
            ae->errorMsg = (char*)Tcl_Alloc(L + 1);
            memcpy(ae->errorMsg, lastEi.message, L);
            ae->errorMsg[L] = '\0';
        }
        ae->done = 1;
        ae->running = 0;
        Tcl_ConditionNotify(&ae->cond);
        Tcl_MutexUnlock(&ae->lock);
    }
    else
    {
        Tcl_MutexLock(&ae->lock);
        ae->rc = 0;
        ae->done = 1;
        ae->running = 0;
        Tcl_ConditionNotify(&ae->cond);
        Tcl_MutexUnlock(&ae->lock);
    }
}

/* =========================================================================
 * Public async commands
 * ========================================================================= */

int Oradpi_Cmd_ExecAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2 || objc > 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int commit = 0;
    if (objc == 3)
    {
        const char* o = Tcl_GetString(objv[2]);
        if (strcmp(o, "-commit") == 0)
            commit = 1;
        else
        {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
            return TCL_ERROR;
        }
    }

    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is not prepared");

    PoolEnsure();
    if (gPool.nThreads == 0)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "failed to create async thread pool");

    int isNew = 0;
    OradpiAsyncEntry* ae = AsyncEnsure(s, &isNew);
    if (!isNew)
    {
        Tcl_MutexLock(&ae->lock);
        int stillRunning = ae->running && !ae->done;
        Tcl_MutexUnlock(&ae->lock);
        if (stillRunning)
            return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement already executing asynchronously");
        AsyncRemove(s);
        ae = AsyncEnsure(s, NULL);
    }

    Tcl_MutexLock(&ae->lock);
    ae->owner = s->owner;
    ae->doCommit = commit;
    ae->autocommit = s->owner->autocommit;
    /* Snapshot failover policy under lock so the worker thread never reads
     * mutable connection config fields (eliminates data race with oraconfig). */
    ae->foMaxAttempts = s->owner->foMaxAttempts;
    ae->foBackoffMs = s->owner->foBackoffMs;
    ae->foBackoffFactor = s->owner->foBackoffFactor;
    ae->foErrorClasses = s->owner->foErrorClasses;
    ae->rc = 0;
    ae->done = 0;
    ae->running = 1;
    ae->canceled = 0;
    ae->joined = 0;
    ae->errorCode = 0;
    if (ae->errorMsg)
    {
        Tcl_Free(ae->errorMsg);
        ae->errorMsg = NULL;
    }
    Tcl_Size klen = 0;
    const char* kstr = Tcl_GetStringFromObj(objv[1], &klen);
    ae->stmtKey = (char*)Tcl_Alloc((size_t)klen + 1);
    memcpy(ae->stmtKey, kstr, (size_t)klen);
    ae->stmtKey[klen] = '\0';
    Tcl_MutexUnlock(&ae->lock);

    if (dpiConn_addRef(s->owner->conn) != DPI_SUCCESS)
    {
        Tcl_Free(ae->stmtKey);
        ae->stmtKey = NULL;
        AsyncRemove(s);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s->owner, "dpiConn_addRef");
    }
    ae->conn = s->owner->conn;

    if (dpiStmt_addRef(s->stmt) != DPI_SUCCESS)
    {
        dpiConn_release(ae->conn);
        ae->conn = NULL;
        Tcl_Free(ae->stmtKey);
        ae->stmtKey = NULL;
        AsyncRemove(s);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_addRef");
    }
    ae->stmt = s->stmt;

    PoolEnqueue(s);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_WaitAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2 || objc > 4)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-timeout ms?");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int timeoutMs = -1;
    if (objc == 4)
    {
        const char* o = Tcl_GetString(objv[2]);
        if (strcmp(o, "-timeout") != 0)
        {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-timeout ms?");
            return TCL_ERROR;
        }
        Tcl_WideInt wTimeout = 0;
        if (Tcl_GetWideIntFromObj(ip, objv[3], &wTimeout) != TCL_OK)
            return TCL_ERROR;
        timeoutMs = (wTimeout > INT_MAX) ? INT_MAX : (int)wTimeout;
    }

    OradpiAsyncEntry* ae = AsyncLookup(s);
    if (!ae)
    {
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    Tcl_MutexLock(&ae->lock);
    if (timeoutMs >= 0)
    {
        Tcl_Time deadline;
        Tcl_GetTime(&deadline);
        deadline.sec += timeoutMs / 1000;
        deadline.usec += (timeoutMs % 1000) * 1000;
        if (deadline.usec >= 1000000)
        {
            deadline.sec++;
            deadline.usec -= 1000000;
        }
        while (!ae->done && ae->running)
        {
            Tcl_ConditionWait(&ae->cond, &ae->lock, &deadline);
            Tcl_Time now;
            Tcl_GetTime(&now);
            if (now.sec > deadline.sec || (now.sec == deadline.sec && now.usec >= deadline.usec))
                break;
        }
        if (!ae->done && ae->running)
        {
            Tcl_MutexUnlock(&ae->lock);
            Oradpi_SetError(ip, (OradpiBase*)s, -3123, "asynchronous command still processing");
            Tcl_SetObjResult(ip, Tcl_NewIntObj(-3123));
            return TCL_OK;
        }
    }
    else
    {
        while (!ae->done && ae->running)
            Tcl_ConditionWait(&ae->cond, &ae->lock, NULL);
    }
    Tcl_MutexUnlock(&ae->lock);

    int rc, errCode;
    char* errMsg = NULL;
    Tcl_MutexLock(&ae->lock);
    rc = ae->rc;
    errCode = ae->errorCode;
    if (ae->errorMsg)
    {
        size_t L = strlen(ae->errorMsg);
        errMsg = (char*)Tcl_Alloc(L + 1);
        memcpy(errMsg, ae->errorMsg, L + 1);
    }
    ae->joined = 1;
    Tcl_MutexUnlock(&ae->lock);

    AsyncRemove(s);

    const char* skey = Tcl_GetString(objv[1]);
    Oradpi_PendingsForget(ip, skey);
    Oradpi_UpdateStmtType(s);

    if (rc != 0)
    {
        if (errMsg)
        {
            Oradpi_SetError(ip, (OradpiBase*)s, errCode ? errCode : -1, errMsg);
            Tcl_Free(errMsg);
        }
        else
            Oradpi_SetError(ip, (OradpiBase*)s, -1, "asynchronous execute failed");
    }
    Tcl_SetObjResult(ip, Tcl_NewIntObj(rc));
    return TCL_OK;
}

int Oradpi_StmtWaitForAsync(OradpiStmt* s, int cancel, int timeoutMs)
{
    OradpiAsyncEntry* ae = AsyncLookup(s);
    if (!ae)
        return 0;

    if (cancel)
    {
        Tcl_MutexLock(&ae->lock);
        dpiConn* localConn = ae->conn;
        ae->canceled = 1;
        Tcl_MutexUnlock(&ae->lock);
        if (localConn)
            (void)dpiConn_breakExecution(localConn);
    }

    Tcl_MutexLock(&ae->lock);
    if (timeoutMs >= 0)
    {
        Tcl_Time deadline;
        Tcl_GetTime(&deadline);
        deadline.sec += timeoutMs / 1000;
        deadline.usec += (timeoutMs % 1000) * 1000;
        if (deadline.usec >= 1000000)
        {
            deadline.sec++;
            deadline.usec -= 1000000;
        }
        while (!ae->done && ae->running)
        {
            Tcl_ConditionWait(&ae->cond, &ae->lock, &deadline);
            Tcl_Time now;
            Tcl_GetTime(&now);
            if (now.sec > deadline.sec || (now.sec == deadline.sec && now.usec >= deadline.usec))
                break;
        }
        if (!ae->done && ae->running)
        {
            Tcl_MutexUnlock(&ae->lock);
            return -3123;
        }
    }
    else
    {
        while (!ae->done && ae->running)
            Tcl_ConditionWait(&ae->cond, &ae->lock, NULL);
    }
    ae->joined = 1;
    Tcl_MutexUnlock(&ae->lock);

    AsyncRemove(s);
    return 0;
}

int Oradpi_StmtIsAsyncBusy(OradpiStmt* s)
{
    OradpiAsyncEntry* ae = AsyncLookup(s);
    if (!ae)
        return 0;
    Tcl_MutexLock(&ae->lock);
    int busy = (ae->running && !ae->done);
    Tcl_MutexUnlock(&ae->lock);
    return busy;
}

void Oradpi_CancelAndJoinAllForConn(Tcl_Interp* ip, OradpiConn* co)
{
    if (!co)
        return;

    int cap = 4, count = 0;
    OradpiStmt** keys = (OradpiStmt**)Tcl_Alloc(sizeof(OradpiStmt*) * cap);
    char** keyNames = (char**)Tcl_Alloc(sizeof(char*) * cap);

    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_MutexUnlock(&gAsyncMutex);
        Tcl_Free((char*)keys);
        Tcl_Free((char*)keyNames);
        return;
    }
    Tcl_HashSearch hs;
    Tcl_HashEntry* e;
    for (e = Tcl_FirstHashEntry(&gAsyncByStmt, &hs); e; e = Tcl_NextHashEntry(&hs))
    {
        OradpiAsyncEntry* ae = (OradpiAsyncEntry*)Tcl_GetHashValue(e);
        if (ae && ae->owner == co)
        {
            if (count == cap)
            {
                cap *= 2;
                keys = (OradpiStmt**)Tcl_Realloc((char*)keys, sizeof(OradpiStmt*) * cap);
                keyNames = (char**)Tcl_Realloc((char*)keyNames, sizeof(char*) * cap);
            }
            keys[count] = (OradpiStmt*)Tcl_GetHashKey(&gAsyncByStmt, e);
            if (ae->stmtKey)
            {
                size_t L = strlen(ae->stmtKey);
                keyNames[count] = (char*)Tcl_Alloc(L + 1);
                memcpy(keyNames[count], ae->stmtKey, L + 1);
            }
            else
                keyNames[count] = NULL;
            count++;
        }
    }
    Tcl_MutexUnlock(&gAsyncMutex);

    for (int i = 0; i < count; i++)
    {
        (void)Oradpi_StmtWaitForAsync(keys[i], 1, -1);
        if (ip && keyNames[i])
        {
            Oradpi_PendingsForget(ip, keyNames[i]);
            Tcl_Free(keyNames[i]);
        }
    }
    if (keys)
        Tcl_Free((char*)keys);
    if (keyNames)
        Tcl_Free((char*)keyNames);
}
