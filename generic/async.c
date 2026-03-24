/*
 *  async.c --
 *
 *    Asynchronous execution support with thread pool.
 *
 *        - Maintains a lazily-initialized pool of persistent worker threads
 *          with a shared work queue, replacing the old thread-per-op design.
 *        - Tracks per-statement async state; provides start/cancel/join and
 *          safe teardown semantics.
 *        - Registry protected by Tcl mutexes; keys are stable string handle
 *          names to avoid ABA hazards from pointer reuse (C-2 fix).
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
#include <string.h>
#ifndef USE_TCL_STUBS
#define USE_TCL_STUBS
#endif
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
    int refcnt;         /* V-1 fix: reference count protected by gAsyncMutex */
    int running;
    int done;
    int canceled;
    int joined;
    int orphaned; /* C-2 fix: set by waiter on timeout; worker self-cleans */
    int rc;
    int errorCode;
    char* errorMsg;
    /* FIX 3 (MAJOR): persist dpiErrorInfo fields needed by orawaitasync so it
     * can call Oradpi_SetErrorFromODPIInfo and restore full failover semantics
     * (isRecoverable flag, failover callback dispatch) on the interp thread. */
    int isRecoverable;

    dpiConn* conn;
    dpiStmt* stmt;
    GlobalConnRec* shared;

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
    char* stmtKey; /* C-2 fix: owned copy of registry key (not a raw pointer) */
    struct PoolWorkItem* next;
} PoolWorkItem;

typedef struct OradpiThreadPool
{
    Tcl_Mutex queueMutex;
    Tcl_Condition queueCond;
    PoolWorkItem* head;
    PoolWorkItem* tail;

    Tcl_ThreadId* threads;
    Tcl_Size nThreads;
    int shutdown;
    int started;

    /* Best-effort shutdown: workers decrement liveWorkers before exiting
     * and signal exitCond so the exit handler can do a bounded wait. */
    int liveWorkers;
    Tcl_Condition exitCond;
} OradpiThreadPool;

#define ORADPI_DEFAULT_POOL_SIZE 4

/* =========================================================================
 * Forward Declarations
 * ========================================================================= */

static OradpiAsyncEntry* AsyncEnsure(const char* key, int* isNewOut);
static OradpiAsyncEntry* AsyncLookup(const char* key);
static void AsyncRegistryEnsure(void);
static void AsyncRemove(const char* key);
static void AsyncRelease(OradpiAsyncEntry* ae);
static void AsyncWorkerBody(const char* key);
void Oradpi_CancelAndJoinAllForConn(Tcl_Interp* ip, OradpiConn* co);
int Oradpi_Cmd_ExecAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_WaitAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_StmtWaitForAsync(OradpiStmt* s, int cancel, int timeoutMs);
int Oradpi_StmtIsAsyncBusy(OradpiStmt* s);

static void PoolEnsure(void);
static Tcl_Size PoolThreadCount(void);
static void PoolEnqueue(const char* key);
static void PoolExitHandler(void* unused);
static Tcl_ThreadCreateProc PoolThreadProc;

/* =========================================================================
 * Globals
 * ========================================================================= */

static Tcl_HashTable gAsyncByKey; /* C-2 fix: string-keyed, not pointer-keyed */
static int gAsyncInit = 0;
/* gAsyncMutex: protects gAsyncByKey and gAsyncInit.
 * Lock ordering: acquire before any per-entry ae->lock. */
static Tcl_Mutex gAsyncMutex;

static OradpiThreadPool gPool;
static int gPoolInited = 0;
/* gPoolInitMutex: protects gPool struct and gPoolInited flag.
 * Lock ordering: leaf lock (no other locks held while this is held). */
static Tcl_Mutex gPoolInitMutex;

/* =========================================================================
 * Async registry — C-2 fix: uses stable string keys (handle names)
 *                  instead of raw OradpiStmt* pointers.
 * ========================================================================= */

static void AsyncRegistryEnsure(void)
{
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_InitHashTable(&gAsyncByKey, TCL_STRING_KEYS);
        gAsyncInit = 1;
    }
    Tcl_MutexUnlock(&gAsyncMutex);
}

static OradpiAsyncEntry* AsyncLookup(const char* key)
{
    if (!key)
        return NULL;
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_MutexUnlock(&gAsyncMutex);
        return NULL;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gAsyncByKey, key);
    OradpiAsyncEntry* ae = he ? (OradpiAsyncEntry*)Tcl_GetHashValue(he) : NULL;
    /* V-1 fix: increment refcount under gAsyncMutex so the entry cannot
     * be freed between lookup and use.  Callers must call AsyncRelease(). */
    if (ae)
        ae->refcnt++;
    Tcl_MutexUnlock(&gAsyncMutex);
    return ae;
}

static OradpiAsyncEntry* AsyncEnsure(const char* key, int* isNewOut)
{
    AsyncRegistryEnsure();
    Tcl_MutexLock(&gAsyncMutex);
    int isNew = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&gAsyncByKey, key, &isNew);
    OradpiAsyncEntry* ae;
    if (isNew)
    {
        ae = (OradpiAsyncEntry*)Tcl_Alloc(sizeof(*ae));
        memset(ae, 0, sizeof(*ae));
        ae->refcnt = 2; /* V-1 fix: one ref for the hash table, one for the caller */
        Tcl_SetHashValue(he, ae);
    }
    else
    {
        ae = (OradpiAsyncEntry*)Tcl_GetHashValue(he);
        ae->refcnt++; /* V-1 fix: caller gets a ref */
    }
    Tcl_MutexUnlock(&gAsyncMutex);
    if (isNewOut)
        *isNewOut = isNew;
    return ae;
}

/* V-1 fix: Free the entry's resources.  Only called when refcount reaches 0. */
static void AsyncFreeEntry(OradpiAsyncEntry* ae)
{
    if (!ae)
        return;
    if (ae->errorMsg)
        Tcl_Free(ae->errorMsg);
    if (ae->stmt)
        dpiStmt_release(ae->stmt);
    if (ae->conn)
        dpiConn_release(ae->conn);
    if (ae->shared)
        Oradpi_SharedConnRelease(ae->shared);
    if (ae->stmtKey)
        Tcl_Free(ae->stmtKey);
    Tcl_ConditionFinalize(&ae->cond);
    Tcl_MutexFinalize(&ae->lock);
    Tcl_Free((char*)ae);
}

/* V-1 fix: Decrement refcount.  When it hits 0, free the entry.
 * Must be called once for each AsyncLookup/AsyncLookupLocked/AsyncEnsure. */
static void AsyncRelease(OradpiAsyncEntry* ae)
{
    if (!ae)
        return;
    Tcl_MutexLock(&gAsyncMutex);
    ae->refcnt--;
    int doFree = (ae->refcnt <= 0);
    Tcl_MutexUnlock(&gAsyncMutex);
    if (doFree)
        AsyncFreeEntry(ae);
}

/* V-1 fix: Remove the entry from the hash table and release the table's
 * reference.  The actual free is deferred until refcount reaches 0. */
static void AsyncRemove(const char* key)
{
    if (!key)
        return;
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_MutexUnlock(&gAsyncMutex);
        return;
    }
    Tcl_HashEntry* he = Tcl_FindHashEntry(&gAsyncByKey, key);
    if (he)
    {
        OradpiAsyncEntry* ae = (OradpiAsyncEntry*)Tcl_GetHashValue(he);
        Tcl_DeleteHashEntry(he);
        if (ae)
        {
            ae->refcnt--;
            int doFree = (ae->refcnt <= 0);
            Tcl_MutexUnlock(&gAsyncMutex);
            if (doFree)
                AsyncFreeEntry(ae);
            return;
        }
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

        /* C-2 fix: work item carries a string key, not a raw pointer */
        char* key = item->stmtKey;
        Tcl_Free((char*)item);
        AsyncWorkerBody(key);
        Tcl_Free(key);
    }

    /* Signal the exit handler that this worker is done */
    Tcl_MutexLock(&gPool.queueMutex);
    if (gPool.liveWorkers > 0)
        gPool.liveWorkers--;
    Tcl_ConditionNotify(&gPool.exitCond);
    Tcl_MutexUnlock(&gPool.queueMutex);
}

static void PoolEnsure(void)
{
    Tcl_MutexLock(&gPoolInitMutex);
    if (gPoolInited)
    {
        Tcl_MutexUnlock(&gPoolInitMutex);
        return;
    }
    size_t threadBytes = 0;
    memset(&gPool, 0, sizeof(gPool));
    gPool.nThreads = ORADPI_DEFAULT_POOL_SIZE;
    if (Oradpi_CheckedAllocBytes(NULL, gPool.nThreads, sizeof(Tcl_ThreadId), &threadBytes, "worker thread table") != TCL_OK)
    {
        Tcl_MutexUnlock(&gPoolInitMutex);
        return;
    }
    gPool.threads = (Tcl_ThreadId*)Tcl_Alloc(threadBytes);
    memset(gPool.threads, 0, threadBytes);

    for (Tcl_Size i = 0; i < gPool.nThreads; i++)
    {
        if (Tcl_CreateThread(&gPool.threads[i], PoolThreadProc, NULL, TCL_THREAD_STACK_DEFAULT, TCL_THREAD_JOINABLE) != TCL_OK)
        {
            gPool.nThreads = i;
            break;
        }
    }

    /* V-10 fix: only mark pool as initialized if at least one worker was
     * created.  If all thread creations failed, roll back fully so a later
     * call to PoolEnsure() can retry.
     * Limitation: Tcl_CreateThread does not expose OS-level error details,
     * so the caller (Cmd_ExecAsync) can only report a generic failure.
     * The caller detects this via PoolThreadCount() == 0. */
    if (gPool.nThreads == 0)
    {
        Tcl_Free((char*)gPool.threads);
        gPool.threads = NULL;
        memset(&gPool, 0, sizeof(gPool));
        Tcl_MutexUnlock(&gPoolInitMutex);
        return;
    }
    gPool.started = 1;
    gPool.liveWorkers = (int)gPool.nThreads;
    gPoolInited = 1;
    Tcl_CreateExitHandler(PoolExitHandler, NULL);
    Tcl_MutexUnlock(&gPoolInitMutex);
}

static Tcl_Size PoolThreadCount(void)
{
    Tcl_Size nThreads = 0;
    Tcl_MutexLock(&gPoolInitMutex);
    if (gPoolInited)
        nThreads = gPool.nThreads;
    Tcl_MutexUnlock(&gPoolInitMutex);
    return nThreads;
}

static void PoolEnqueue(const char* key)
{
    /* C-2 fix: enqueue a copy of the string key, not a raw pointer */
    Tcl_Size klen = (Tcl_Size)strlen(key);
    size_t keyBytes = 0;
    /* Cannot fail for reasonable key lengths; clamp defensively */
    if (Oradpi_CheckedAllocBytes(NULL, klen + 1, sizeof(char), &keyBytes, NULL) != TCL_OK)
        return;
    char* keyCopy = (char*)Tcl_Alloc(keyBytes);
    memcpy(keyCopy, key, (size_t)klen + 1);

    PoolWorkItem* item = (PoolWorkItem*)Tcl_Alloc(sizeof(*item));
    item->stmtKey = keyCopy;
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
    Tcl_Size nThreads = 0;

    (void)unused;
    Tcl_MutexLock(&gPoolInitMutex);
    if (!gPoolInited)
    {
        Tcl_MutexUnlock(&gPoolInitMutex);
        goto registry_cleanup;
    }
    nThreads = gPool.nThreads;
    Tcl_MutexUnlock(&gPoolInitMutex);

    /* FIX 2 (MAJOR): Two-phase break — collect (shared, conn) pairs under
     * gAsyncMutex, then call Oradpi_SharedConnBreak *outside* that lock.
     * Holding gAsyncMutex while calling into ODPI/OCI (which can block on
     * network or driver state) risks deadlock and long stalls at shutdown. */
    typedef struct { GlobalConnRec* shared; dpiConn* conn; } BreakItem;
    BreakItem* breakList = NULL;
    int breakCap = 0, breakCount = 0;

    Tcl_MutexLock(&gAsyncMutex);
    if (gAsyncInit)
    {
        Tcl_HashSearch hs;
        Tcl_HashEntry* e;
        for (e = Tcl_FirstHashEntry(&gAsyncByKey, &hs); e; e = Tcl_NextHashEntry(&hs))
        {
            OradpiAsyncEntry* ae = (OradpiAsyncEntry*)Tcl_GetHashValue(e);
            if (!ae)
                continue;
            Tcl_MutexLock(&ae->lock);
            if (ae->running && !ae->done && ae->conn)
            {
                ae->canceled = 1;
                /* Grow list if needed.  Tcl_Realloc panics on OOM in Tcl 9;
                 * a NULL check here would imply a fallible allocator and
                 * mislead future readers about the actual failure mode. */
                if (breakCount >= breakCap)
                {
                    int newCap = breakCap ? breakCap * 2 : 8;
                    breakList = (BreakItem*)Tcl_Realloc(
                        (char*)breakList, (unsigned)newCap * sizeof(BreakItem));
                    breakCap = newCap;
                }
                breakList[breakCount].shared = ae->shared;
                breakList[breakCount].conn   = ae->conn;
                breakCount++;
            }
            Tcl_MutexUnlock(&ae->lock);
        }
    }
    Tcl_MutexUnlock(&gAsyncMutex);

    /* Phase 2: call into ODPI/OCI with no global locks held */
    for (int bi = 0; bi < breakCount; bi++)
        Oradpi_SharedConnBreak(breakList[bi].shared, breakList[bi].conn);
    if (breakList)
        Tcl_Free((char*)breakList);

    Tcl_MutexLock(&gPool.queueMutex);
    gPool.shutdown = 1;
    for (Tcl_Size i = 0; i < nThreads; i++)
        Tcl_ConditionNotify(&gPool.queueCond);
    Tcl_MutexUnlock(&gPool.queueMutex);

    /* Best-effort bounded wait: give workers up to ORADPI_TEARDOWN_TIMEOUT_MS
     * to exit.  If a worker is stuck in an OCI call that breakExecution could
     * not interrupt, we skip joining it rather than blocking process exit
     * indefinitely. */
    Tcl_Time deadline;
    Tcl_GetTime(&deadline);
    deadline.sec += ORADPI_TEARDOWN_TIMEOUT_MS / 1000;
    deadline.usec += (ORADPI_TEARDOWN_TIMEOUT_MS % 1000) * 1000;
    while (deadline.usec >= 1000000)
    {
        deadline.sec++;
        deadline.usec -= 1000000;
    }

    Tcl_MutexLock(&gPool.queueMutex);
    while (gPool.liveWorkers > 0)
    {
        Tcl_ConditionWait(&gPool.exitCond, &gPool.queueMutex, &deadline);
        if (gPool.liveWorkers == 0)
            break;
        Tcl_Time now;
        Tcl_GetTime(&now);
        if (now.sec > deadline.sec || (now.sec == deadline.sec && now.usec >= deadline.usec))
            break; /* timeout — stuck workers will be killed by process exit */
    }
    int remaining = gPool.liveWorkers;
    Tcl_MutexUnlock(&gPool.queueMutex);

    /* Only attempt Tcl_JoinThread for workers that have actually exited.
     * If any are still stuck, skip joining them — process exit will
     * terminate them. */
    if (remaining == 0)
    {
        for (Tcl_Size i = 0; i < nThreads; i++)
        {
            if (gPool.threads[i])
                (void)Tcl_JoinThread(gPool.threads[i], NULL);
        }
    }

    Tcl_MutexLock(&gPool.queueMutex);
    while (gPool.head)
    {
        PoolWorkItem* item = gPool.head;
        gPool.head = item->next;
        if (item->stmtKey)
            Tcl_Free(item->stmtKey);
        Tcl_Free((char*)item);
    }
    Tcl_MutexUnlock(&gPool.queueMutex);

    if (remaining == 0)
    {
        Tcl_ConditionFinalize(&gPool.exitCond);
        Tcl_ConditionFinalize(&gPool.queueCond);
        Tcl_MutexFinalize(&gPool.queueMutex);
    }
    /* else: skip finalization — stuck workers may still reference these */

    if (gPool.threads)
    {
        Tcl_Free((char*)gPool.threads);
        gPool.threads = NULL;
    }

    Tcl_MutexLock(&gPoolInitMutex);
    gPool.nThreads = 0;
    gPool.started = 0;
    gPoolInited = 0;
    Tcl_MutexUnlock(&gPoolInitMutex);

registry_cleanup:
    /* C-2 / V-8 fix: Clean up async registry entries at process exit.
     * When all workers have exited (remaining == 0), every entry is safe
     * to free.  When stuck workers remain, only free entries whose worker
     * has completed (ae->done).  Entries with a still-running worker must
     * be left alone — their memory will be reclaimed by process exit.
     * Freeing them here while the worker is active is use-after-free. */
    Tcl_MutexLock(&gAsyncMutex);
    if (gAsyncInit)
    {
        Tcl_HashSearch hs;
        Tcl_HashEntry* e;
        for (e = Tcl_FirstHashEntry(&gAsyncByKey, &hs); e; e = Tcl_NextHashEntry(&hs))
        {
            OradpiAsyncEntry* ae = (OradpiAsyncEntry*)Tcl_GetHashValue(e);
            if (!ae)
                continue;
            if (remaining > 0)
            {
                /* Workers still alive — only free completed entries */
                Tcl_MutexLock(&ae->lock);
                int done = ae->done;
                Tcl_MutexUnlock(&ae->lock);
                if (!done)
                    continue; /* worker still running; skip */
            }
            AsyncFreeEntry(ae);
        }
        Tcl_DeleteHashTable(&gAsyncByKey);
        gAsyncInit = 0;
    }
    Tcl_MutexUnlock(&gAsyncMutex);
}

/* =========================================================================
 * Worker body (runs on pool thread)
 *
 * S-5 contract: Worker threads MUST NOT call any Tcl_Interp-bound API.
 * All Tcl_Obj and interp operations are performed on the interpreter
 * thread.  Workers only use Tcl_Mutex/Tcl_Condition, Tcl_Alloc/Tcl_Free,
 * Tcl_Sleep, and ODPI-C APIs on their own addRef'd handles.
 * ========================================================================= */

static void AsyncWorkerBody(const char* key)
{
    OradpiAsyncEntry* ae = AsyncLookup(key);
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
    memset(&info, 0, sizeof(info));
    Oradpi_SharedConnGateEnter(ae->shared);
    if (dpiStmt_getInfo(stmt, &info) == DPI_SUCCESS)
    {
        if (doCommit || (autocommit && (info.isDML || info.isPLSQL)))
            mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;
    }
    Oradpi_SharedConnGateLeave(ae->shared);

    /* Execute with retry/backoff if failover policy is configured (SUG-4).
     * maxAttempts/backoffMs/backoffFact/errClasses are already snapshotted above. */

    int execRc = DPI_FAILURE;
    uint32_t nqc = 0;
    dpiErrorInfo lastEi;
    memset(&lastEi, 0, sizeof(lastEi));

    uint32_t attempt = 0;
    /* V-6 fix: overflow-safe totalTries computation */
    uint32_t totalTries;
    if (maxAttempts > 0 && errClasses)
        totalTries = (maxAttempts >= UINT32_MAX) ? UINT32_MAX : maxAttempts + 1;
    else
        totalTries = 1;

    for (attempt = 0; attempt < totalTries; attempt++)
    {
        /* Check if canceled between retries */
        Tcl_MutexLock(&ae->lock);
        int wasCanceled = ae->canceled;
        Tcl_MutexUnlock(&ae->lock);
        if (wasCanceled)
            break;

        nqc = 0;
        Oradpi_SharedConnGateEnter(ae->shared);
        execRc = dpiStmt_execute(stmt, mode, &nqc);
        Oradpi_SharedConnGateLeave(ae->shared);
        if (execRc == DPI_SUCCESS)
            break;

        /* Capture error immediately (thread-local) */
        memset(&lastEi, 0, sizeof(lastEi));
        (void)Oradpi_CaptureODPIError(&lastEi);

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
            /* M-7: Clamp to non-negative (NaN or negative backoffFact) */
            if (!(sleepMs >= 0.0))
                sleepMs = 0.0;
            Tcl_Sleep((int)sleepMs);
        }
    }

    /* --- Completion: single exit path for both success and failure --- */

    int finalRc = 0;
    int finalErrCode = 0;
    char* finalErrMsg = NULL;

    if (execRc != DPI_SUCCESS)
    {
        /* If we didn't capture the error yet (single attempt path) */
        if (lastEi.message == NULL)
            (void)Oradpi_CaptureODPIError(&lastEi);

        finalRc = -1;
        finalErrCode = (int)lastEi.code;
        if (lastEi.message && lastEi.messageLength > 0)
        {
            Tcl_Size msgLen = (Tcl_Size)lastEi.messageLength;
            size_t msgBytes = 0;
            if (Oradpi_CheckedAllocBytes(NULL, msgLen + 1, sizeof(char), &msgBytes, "async error message") == TCL_OK)
            {
                finalErrMsg = (char*)Tcl_Alloc(msgBytes);
                memcpy(finalErrMsg, lastEi.message, (size_t)msgLen);
                finalErrMsg[msgLen] = '\0';
            }
        }
    }

    Tcl_MutexLock(&ae->lock);
    ae->rc = finalRc;
    ae->errorCode = finalErrCode;
    ae->errorMsg = finalErrMsg;
    /* FIX 3: persist recoverability so orawaitasync can fire failover callbacks */
    ae->isRecoverable = (execRc != DPI_SUCCESS) ? lastEi.isRecoverable : 0;
    ae->done = 1;
    ae->running = 0;
    int wasOrphaned = ae->orphaned;
    Tcl_ConditionNotify(&ae->cond);
    Tcl_MutexUnlock(&ae->lock);

    /* C-2 fix: If the waiter timed out and set orphaned, the worker is
     * responsible for cleaning up the registry entry.  The waiter has
     * already moved on — no one else will call AsyncRemove for this key. */
    if (wasOrphaned)
        AsyncRemove(key);

    /* V-1 fix: release the reference obtained by AsyncLookup at entry */
    AsyncRelease(ae);
}

/* =========================================================================
 * Public async commands
 * ========================================================================= */

/*
 * oraexecasync statement-handle ?-commit?
 *
 *   Submits a statement for asynchronous execution on the thread pool.
 *   The statement must be prepared and bound before this call. Failover
 *   policy fields are snapshotted at enqueue time to avoid data races.
 *   Returns: 0 on successful submission.
 *   Errors:  invalid/unprepared handle; already executing; pool creation failure.
 *   Thread-safety: safe — snapshots connection state under lock before dispatch.
 */
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
        static const char* const execAsyncOpts[] = {"-commit", NULL};
        int optIdx = 0;
        if (Tcl_GetIndexFromObj(ip, objv[2], execAsyncOpts, "option", 0, &optIdx) != TCL_OK)
            return TCL_ERROR;
        commit = 1;
    }

    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is not prepared");

    PoolEnsure();
    if (PoolThreadCount() == 0)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1,
            "failed to create async thread pool (Tcl_CreateThread failed; check system thread limits)");

    /* C-2 fix: use stable string key (handle name) instead of raw pointer */
    const char* key = Tcl_GetString(s->base.name);

    int isNew = 0;
    OradpiAsyncEntry* ae = AsyncEnsure(key, &isNew);
    if (!isNew)
    {
        Tcl_MutexLock(&ae->lock);
        int stillRunning = ae->running && !ae->done;
        Tcl_MutexUnlock(&ae->lock);
        if (stillRunning)
        {
            AsyncRelease(ae); /* V-1 fix: release ref from AsyncEnsure */
            return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement already executing asynchronously");
        }
        AsyncRemove(key);
        AsyncRelease(ae); /* V-1 fix: release ref from first AsyncEnsure */
        ae = AsyncEnsure(key, NULL);
    }

    Tcl_Size klen = 0;
    const char* kstr = Tcl_GetStringFromObj(objv[1], &klen);
    size_t keyBytes = 0;
    char* stmtKeyCopy = NULL;
    if (Oradpi_CheckedAllocBytes(ip, klen + 1, sizeof(char), &keyBytes, "async statement key") != TCL_OK)
    {
        AsyncRemove(key);
        AsyncRelease(ae); /* V-1 fix */
        return TCL_ERROR;
    }
    stmtKeyCopy = (char*)Tcl_Alloc(keyBytes);
    memcpy(stmtKeyCopy, kstr, (size_t)klen);
    stmtKeyCopy[klen] = '\0';

    if (dpiConn_addRef(s->owner->conn) != DPI_SUCCESS)
    {
        Tcl_Free(stmtKeyCopy);
        AsyncRemove(key);
        AsyncRelease(ae); /* V-1 fix */
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s->owner, "dpiConn_addRef");
    }

    if (dpiStmt_addRef(s->stmt) != DPI_SUCCESS)
    {
        dpiConn_release(s->owner->conn);
        Tcl_Free(stmtKeyCopy);
        AsyncRemove(key);
        AsyncRelease(ae); /* V-1 fix */
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_addRef");
    }

    Oradpi_SharedConnAddRef(s->owner->shared);

    Tcl_MutexLock(&ae->lock);
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
    ae->orphaned = 0;
    ae->errorCode = 0;
    if (ae->errorMsg)
    {
        Tcl_Free(ae->errorMsg);
        ae->errorMsg = NULL;
    }
    ae->conn = s->owner->conn;
    ae->stmt = s->stmt;
    ae->shared = s->owner->shared;
    ae->stmtKey = stmtKeyCopy;
    Tcl_MutexUnlock(&ae->lock);

    PoolEnqueue(key);

    /* V-1 fix: release caller's ref; the hash table still holds one,
     * and the worker will acquire its own via AsyncLookup. */
    AsyncRelease(ae);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

/*
 * orawaitasync statement-handle ?-timeout ms?
 *
 *   Blocks until the asynchronous execution on the given statement
 *   completes, or until the optional timeout (in milliseconds) expires.
 *   Returns: 0 on success; -3123 on timeout; error code on exec failure.
 *   Errors:  invalid handle; propagated ODPI-C execution errors.
 *   Thread-safety: safe — waits on per-entry condition variable.
 */
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
        /* S-3 fix: reject negative values < -1 as invalid */
        if (wTimeout < -1)
        {
            Tcl_SetObjResult(ip, Tcl_NewStringObj("orawaitasync: -timeout must be >= 0 (or -1 for infinite)", -1));
            return TCL_ERROR;
        }
        if (wTimeout > INT_MAX)
            return Oradpi_SetError(ip, (OradpiBase*)s, -1, "orawaitasync: -timeout exceeds INT_MAX milliseconds");
        timeoutMs = (int)wTimeout;
    }

    /* C-2 fix: use string key instead of raw pointer */
    const char* key = Tcl_GetString(s->base.name);

    OradpiAsyncEntry* ae = AsyncLookup(key);
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
        /* m-9: Normalizing loop handles usec values up to ~1999999
         * (when Tcl_GetTime returns usec near 999999 and timeout adds 999000). */
        while (deadline.usec >= 1000000)
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
            AsyncRelease(ae); /* V-1 fix */
            /* Store timeout detail in statement's OradpiMsg for oramsg,
             * but do NOT set Tcl errorCode since we return TCL_OK. */
            if (s)
            {
                s->base.msg.rc = -3123;
                s->base.msg.ocicode = -3123;
            }
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
    int isRecoverable = 0;
    char* errMsg = NULL;
    Tcl_MutexLock(&ae->lock);
    rc = ae->rc;
    errCode = ae->errorCode;
    isRecoverable = ae->isRecoverable;
    if (ae->errorMsg)
    {
        /* V-6 fix: pass NULL interp to avoid calling Tcl_SetObjResult
         * while holding ae->lock (deadlock hazard). */
        Tcl_Size msgLen = (Tcl_Size)strlen(ae->errorMsg);
        size_t msgBytes = 0;
        if (Oradpi_CheckedAllocBytes(NULL, msgLen + 1, sizeof(char), &msgBytes, "async wait error message") == TCL_OK)
        {
            errMsg = (char*)Tcl_Alloc(msgBytes);
            memcpy(errMsg, ae->errorMsg, msgLen + 1);
        }
    }
    ae->joined = 1;
    Tcl_MutexUnlock(&ae->lock);

    AsyncRemove(key);
    AsyncRelease(ae); /* V-1 fix */

    const char* skey = Tcl_GetString(objv[1]);
    Oradpi_PendingsForget(ip, skey);
    Oradpi_UpdateStmtType(s);

    /* V-8 fix: Record rows affected on async success.  The sync execute path
     * calls dpiStmt_getRowCount + Oradpi_RecordRows, but the async worker
     * did not, and orawaitasync didn't either.  Without this, "oramsg $S rows"
     * after oraexecasync/orawaitasync reported stale data. */
    if (rc == 0 && s->stmt && s->owner)
    {
        CONN_GATE_ENTER(s->owner);
        uint64_t rows = 0;
        if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
            Oradpi_RecordRows((OradpiBase*)s, rows);
        CONN_GATE_LEAVE(s->owner);
    }

    if (rc != 0)
    {
        /* FIX 3 (MAJOR): Reconstruct a minimal dpiErrorInfo on the interp thread
         * from the fields the worker persisted in OradpiAsyncEntry, then call
         * Oradpi_SetErrorFromODPIInfo(NULL, ...) so that:
         *   (a) h->msg.recoverable is set correctly from ae->isRecoverable, and
         *   (b) if the error is recoverable and a failover callback is configured,
         *       Oradpi_PostFailoverEvent fires from the connection's owner thread
         *       (the only thread permitted to touch those Tcl_Obj fields safely).
         * NULL interp keeps the interp result and errorCode clean — the numeric
         * resultCode below is the sole error signal per the orawaitasync contract. */
        dpiErrorInfo ei;
        memset(&ei, 0, sizeof(ei));
        ei.code          = (int32_t)(errCode ? errCode : -1);
        ei.isRecoverable = isRecoverable;
        ei.message       = errMsg ? errMsg : "asynchronous execute failed";
        ei.messageLength = (uint32_t)strlen(ei.message);
        Oradpi_SetErrorFromODPIInfo(NULL, (OradpiBase*)s, "oraexecasync", &ei);
        if (errMsg)
            Tcl_Free(errMsg);
    }
    /* V-6 fix: return the actual Oracle/ODPI error code on failure, not the
     * worker's internal -1 sentinel.  This matches the documented contract:
     * "Returns: 0 on success; -3123 on timeout; error code on exec failure." */
    int resultCode = (rc != 0) ? (errCode ? errCode : -1) : 0;
    Tcl_SetObjResult(ip, Tcl_NewIntObj(resultCode));
    return TCL_OK;
}

int Oradpi_StmtWaitForAsync(OradpiStmt* s, int cancel, int timeoutMs)
{
    if (!s || !s->base.name)
        return 0;
    const char* key = Tcl_GetString(s->base.name);

    OradpiAsyncEntry* ae = AsyncLookup(key);
    if (!ae)
        return 0;

    if (cancel)
    {
        Tcl_MutexLock(&ae->lock);
        dpiConn* localConn = ae->conn;
        ae->canceled = 1;
        Tcl_MutexUnlock(&ae->lock);
        if (localConn)
            Oradpi_SharedConnBreak(ae->shared, localConn);
    }

    Tcl_MutexLock(&ae->lock);
    if (timeoutMs >= 0)
    {
        Tcl_Time deadline;
        Tcl_GetTime(&deadline);
        deadline.sec += timeoutMs / 1000;
        deadline.usec += (timeoutMs % 1000) * 1000;
        /* m-9: Normalizing loop (same fix as WaitAsync) */
        while (deadline.usec >= 1000000)
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
            /* C-2 fix: mark as orphaned so the worker self-cleans.
             * Do NOT call AsyncRemove — the worker still holds a
             * pointer to this ae and will write to it on completion. */
            ae->orphaned = 1;
            Tcl_MutexUnlock(&ae->lock);
            AsyncRelease(ae); /* V-1 fix */
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

    AsyncRemove(key);
    AsyncRelease(ae); /* V-1 fix */
    return 0;
}

int Oradpi_StmtIsAsyncBusy(OradpiStmt* s)
{
    if (!s || !s->base.name)
        return 0;
    const char* key = Tcl_GetString(s->base.name);
    OradpiAsyncEntry* ae = AsyncLookup(key);
    if (!ae)
        return 0;
    Tcl_MutexLock(&ae->lock);
    int busy = (ae->running && !ae->done);
    Tcl_MutexUnlock(&ae->lock);
    AsyncRelease(ae); /* V-1 fix */
    return busy;
}

void Oradpi_CancelAndJoinAllForConn(Tcl_Interp* ip, OradpiConn* co)
{
    if (!co)
        return;

    Tcl_Size cap = 4, count = 0;
    size_t keysBytes = 0;
    size_t keyNamesBytes = 0;
    size_t entriesBytes = 0;
    /* C-2 fix: collect string keys instead of raw pointers.
     * V-1 fix: also collect refcounted ae pointers so we don't need
     * redundant lookups in the cancel/wait loop. */
    if (Oradpi_CheckedAllocBytes(NULL, cap, sizeof(char*), &keysBytes, "async cancel key table") != TCL_OK ||
        Oradpi_CheckedAllocBytes(NULL, cap, sizeof(char*), &keyNamesBytes, "async cancel statement-key table") != TCL_OK ||
        Oradpi_CheckedAllocBytes(NULL, cap, sizeof(OradpiAsyncEntry*), &entriesBytes, "async cancel entry table") != TCL_OK)
        return;
    char** keys = (char**)Tcl_Alloc(keysBytes);
    char** keyNames = (char**)Tcl_Alloc(keyNamesBytes);
    OradpiAsyncEntry** entries = (OradpiAsyncEntry**)Tcl_Alloc(entriesBytes);
    memset(keys, 0, keysBytes);
    memset(keyNames, 0, keyNamesBytes);
    memset(entries, 0, entriesBytes);

    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit)
    {
        Tcl_MutexUnlock(&gAsyncMutex);
        Tcl_Free((char*)keys);
        Tcl_Free((char*)keyNames);
        Tcl_Free((char*)entries);
        return;
    }
    Tcl_HashSearch hs;
    Tcl_HashEntry* e;
    for (e = Tcl_FirstHashEntry(&gAsyncByKey, &hs); e; e = Tcl_NextHashEntry(&hs))
    {
        OradpiAsyncEntry* ae = (OradpiAsyncEntry*)Tcl_GetHashValue(e);
        if (!ae)
            continue;

        /* V-2 / V-8 fix: acquire ae->lock while holding gAsyncMutex (respecting
         * lock order) to safely read mutable fields.  Compare via the stable
         * GlobalConnRec* shared identity instead of the raw OradpiConn* wrapper
         * pointer, which can be freed and reused (ABA risk). */
        Tcl_MutexLock(&ae->lock);
        if (ae->shared != co->shared)
        {
            Tcl_MutexUnlock(&ae->lock);
            continue;
        }

        if (count == cap)
        {
            Tcl_Size newCap = 0;
            size_t newKeysBytes = 0;
            size_t newKeyNamesBytes = 0;
            size_t newEntriesBytes = 0;
            if (cap > TCL_SIZE_MAX / 2 ||
                Oradpi_CheckedAllocBytes(NULL, (newCap = cap * 2), sizeof(char*), &newKeysBytes, "async cancel key table") !=
                    TCL_OK ||
                Oradpi_CheckedAllocBytes(NULL, newCap, sizeof(char*), &newKeyNamesBytes, "async cancel statement-key table") !=
                    TCL_OK ||
                Oradpi_CheckedAllocBytes(NULL, newCap, sizeof(OradpiAsyncEntry*), &newEntriesBytes, "async cancel entry table") !=
                    TCL_OK)
            {
                Tcl_MutexUnlock(&ae->lock);
                continue;
            }
            keys = (char**)Tcl_Realloc((char*)keys, newKeysBytes);
            keyNames = (char**)Tcl_Realloc((char*)keyNames, newKeyNamesBytes);
            entries = (OradpiAsyncEntry**)Tcl_Realloc((char*)entries, newEntriesBytes);
            memset(keys + cap, 0, sizeof(char*) * (size_t)(newCap - cap));
            memset(keyNames + cap, 0, sizeof(char*) * (size_t)(newCap - cap));
            memset(entries + cap, 0, sizeof(OradpiAsyncEntry*) * (size_t)(newCap - cap));
            cap = newCap;
        }
        /* Copy the hash key string for use after releasing gAsyncMutex.
         * V-6 fix: pass NULL interp to avoid calling Tcl_SetObjResult
         * while holding gAsyncMutex + ae->lock (deadlock hazard). */
        const char* hashKey = Tcl_GetHashKey(&gAsyncByKey, e);
        Tcl_Size hkLen = (Tcl_Size)strlen(hashKey);
        size_t hkBytes = 0;
        if (Oradpi_CheckedAllocBytes(NULL, hkLen + 1, sizeof(char), &hkBytes, "async cancel key copy") != TCL_OK)
        {
            keys[count] = NULL;
        }
        else
        {
            keys[count] = (char*)Tcl_Alloc(hkBytes);
            memcpy(keys[count], hashKey, hkLen + 1);
        }

        /* Copy stmtKey for PendingsForget (now safely under ae->lock).
         * V-6 fix: pass NULL interp — same deadlock avoidance. */
        if (ae->stmtKey)
        {
            Tcl_Size keyLen = (Tcl_Size)strlen(ae->stmtKey);
            size_t keyBytes = 0;
            if (Oradpi_CheckedAllocBytes(NULL, keyLen + 1, sizeof(char), &keyBytes, "async statement key copy") != TCL_OK)
            {
                keyNames[count] = NULL;
            }
            else
            {
                keyNames[count] = (char*)Tcl_Alloc(keyBytes);
                memcpy(keyNames[count], ae->stmtKey, keyLen + 1);
            }
        }
        else
            keyNames[count] = NULL;

        /* V-1 fix: hold a ref so the entry survives until we process it */
        ae->refcnt++;
        entries[count] = ae;

        Tcl_MutexUnlock(&ae->lock);
        count++;
    }
    Tcl_MutexUnlock(&gAsyncMutex);

    /* C-1 fix: use finite timeout instead of infinite wait.
     * If the worker doesn't complete within ORADPI_TEARDOWN_TIMEOUT_MS,
     * the orphan mechanism (C-2) ensures the worker self-cleans. */
    for (Tcl_Size i = 0; i < count; i++)
    {
        if (keys[i])
        {
            OradpiAsyncEntry* ae = entries[i];

            /* Cancel: set flag + break execution */
            Tcl_MutexLock(&ae->lock);
            dpiConn* localConn = ae->conn;
            ae->canceled = 1;
            Tcl_MutexUnlock(&ae->lock);
            if (localConn)
                Oradpi_SharedConnBreak(ae->shared, localConn);

            /* Wait with finite timeout */
            Tcl_MutexLock(&ae->lock);
            Tcl_Time deadline;
            Tcl_GetTime(&deadline);
            deadline.sec += ORADPI_TEARDOWN_TIMEOUT_MS / 1000;
            deadline.usec += (ORADPI_TEARDOWN_TIMEOUT_MS % 1000) * 1000;
            while (deadline.usec >= 1000000)
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
            int wasOrphaned = 0;
            if (!ae->done && ae->running)
            {
                /* Timed out — mark orphaned so worker self-cleans */
                ae->orphaned = 1;
                wasOrphaned = 1;
                Tcl_MutexUnlock(&ae->lock);
            }
            else
            {
                ae->joined = 1;
                Tcl_MutexUnlock(&ae->lock);
                AsyncRemove(keys[i]);
            }
            AsyncRelease(ae); /* V-1 fix: release the ref from the scan loop */

            /* V-8 fix: Only release bind-side pending refs when the worker
             * was successfully joined.  If the entry was orphaned (worker
             * still running), the worker may still be inside dpiStmt_execute()
             * using these dpiVar* references — freeing them here would be
             * use-after-free.  The worker will clean up on completion. */
            if (!wasOrphaned && ip && keyNames[i])
            {
                Oradpi_PendingsForget(ip, keyNames[i]);
            }
            Tcl_Free(keys[i]);
        }
        else
        {
            /* keys[i] was NULL (alloc failure) but we still hold a ref */
            AsyncRelease(entries[i]);
        }
        if (keyNames[i])
            Tcl_Free(keyNames[i]);
    }
    if (keys)
        Tcl_Free((char*)keys);
    if (keyNames)
        Tcl_Free((char*)keyNames);
    if (entries)
        Tcl_Free((char*)entries);
}
