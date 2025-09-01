/*
 *  async.c --
 *
 *    Asynchronous execution support.
 *
 *        - Tracks worker threads per statement; provides start/cancel/join and safe teardown semantics.
 *        - Registry protected by Tcl mutexes; keys are internal pointers to avoid cross‑interp naming clashes.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <stdlib.h>
#include <string.h>
#include <tcl.h>

#include "cmd_int.h"
#include "dpi.h"
#include "state.h"

extern dpiContext *Oradpi_GlobalDpiContext;
void               Oradpi_PendingsForget(Tcl_Interp *ip, const char *stmtKey);

typedef struct OradpiAsyncEntry {
    Tcl_ThreadId tid;
    Tcl_Mutex    lock;
    int          running;
    int          done;
    int          canceled;
    int          joined;
    int          rc;
    int          errorCode;
    char        *errorMsg;

    dpiConn     *conn;
    dpiStmt     *stmt;
    OradpiConn  *owner;

    int          doCommit;
    int          autocommit;

    char        *stmtKey;
} OradpiAsyncEntry;

static Tcl_HashTable gAsyncByStmt;
static int           gAsyncInit = 0;
static Tcl_Mutex     gAsyncMutex;

static void          AsyncRegistryEnsure(void) {
    Tcl_MutexLock(&gAsyncMutex);
    if (!gAsyncInit) {
        Tcl_InitHashTable(&gAsyncByStmt, TCL_ONE_WORD_KEYS);
        gAsyncInit = 1;
    }
    Tcl_MutexUnlock(&gAsyncMutex);
}

static OradpiAsyncEntry *AsyncLookup(OradpiStmt *s) {
    if (!gAsyncInit)
        return NULL;
    Tcl_MutexLock(&gAsyncMutex);
    Tcl_HashEntry    *he = Tcl_FindHashEntry(&gAsyncByStmt, (const void *)s);
    OradpiAsyncEntry *ae = he ? (OradpiAsyncEntry *)Tcl_GetHashValue(he) : NULL;
    Tcl_MutexUnlock(&gAsyncMutex);
    return ae;
}

static OradpiAsyncEntry *AsyncEnsure(OradpiStmt *s, int *isNewOut) {
    AsyncRegistryEnsure();
    Tcl_MutexLock(&gAsyncMutex);
    int            isNew = 0;
    Tcl_HashEntry *he    = Tcl_CreateHashEntry(&gAsyncByStmt, (const void *)s, &isNew);
    if (isNew) {
        OradpiAsyncEntry *ae = (OradpiAsyncEntry *)Tcl_Alloc(sizeof(*ae));
        memset(ae, 0, sizeof(*ae));
        Tcl_SetHashValue(he, ae);
    }
    Tcl_MutexUnlock(&gAsyncMutex);
    if (isNewOut)
        *isNewOut = isNew;
    return AsyncLookup(s);
}

static void AsyncRemove(OradpiStmt *s) {
    if (!gAsyncInit)
        return;
    Tcl_MutexLock(&gAsyncMutex);
    Tcl_HashEntry *he = Tcl_FindHashEntry(&gAsyncByStmt, (const void *)s);
    if (he) {
        OradpiAsyncEntry *ae = (OradpiAsyncEntry *)Tcl_GetHashValue(he);
        if (ae) {
            if (ae->errorMsg)
                Tcl_Free(ae->errorMsg);
            if (ae->stmt)
                dpiStmt_release(ae->stmt);
            if (ae->conn)
                dpiConn_release(ae->conn);
            if (ae->stmtKey)
                Tcl_Free(ae->stmtKey);
            Tcl_Free((char *)ae);
        }
        Tcl_DeleteHashEntry(he);
    }
    Tcl_MutexUnlock(&gAsyncMutex);
}

typedef struct OradpiAsyncArg {
    OradpiStmt *s;
} OradpiAsyncArg;

static void AsyncWorker(void *cd) {
    OradpiAsyncArg *a = (OradpiAsyncArg *)cd;
    OradpiStmt     *s = a->s;
    Tcl_Free((char *)a);

    OradpiAsyncEntry *ae = AsyncLookup(s);
    if (!ae)
        return;

    dpiStmt    *stmt       = ae->stmt;
    int         doCommit   = ae->doCommit;
    int         autocommit = ae->autocommit;

    dpiExecMode mode       = DPI_MODE_EXEC_DEFAULT;
    dpiStmtInfo info;
    if (dpiStmt_getInfo(stmt, &info) == DPI_SUCCESS) {
        if (doCommit || (autocommit && (info.isDML || info.isPLSQL)))
            mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;
    }

    uint32_t nqc = 0;
    if (dpiStmt_execute(stmt, mode, &nqc) != DPI_SUCCESS) {
        dpiErrorInfo ei;
        memset(&ei, 0, sizeof(ei));
        if (Oradpi_GlobalDpiContext)
            dpiContext_getError(Oradpi_GlobalDpiContext, &ei);

        Tcl_MutexLock(&ae->lock);
        ae->rc        = -1;
        ae->errorCode = (int)ei.code;
        if (ei.message && ei.messageLength > 0) {
            size_t L     = (size_t)ei.messageLength;
            ae->errorMsg = (char *)Tcl_Alloc(L + 1);
            memcpy(ae->errorMsg, ei.message, L);
            ae->errorMsg[L] = '\0';
        }
        Tcl_MutexUnlock(&ae->lock);
    } else {
        Tcl_MutexLock(&ae->lock);
        ae->rc = 0;
        Tcl_MutexUnlock(&ae->lock);
    }

    Tcl_MutexLock(&ae->lock);
    ae->done    = 1;
    ae->running = 0;
    Tcl_MutexUnlock(&ae->lock);
}

int Oradpi_Cmd_ExecAsync(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2 || objc > 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int commit = 0;
    if (objc == 3) {
        const char *o = Tcl_GetString(objv[2]);
        if (strcmp(o, "-commit") == 0)
            commit = 1;
        else {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
            return TCL_ERROR;
        }
    }

    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "statement is not prepared");

    int               isNew = 0;
    OradpiAsyncEntry *ae    = AsyncEnsure(s, &isNew);
    if (!isNew) {
        Tcl_MutexLock(&ae->lock);
        int stillRunning = ae->running && !ae->done;
        Tcl_MutexUnlock(&ae->lock);
        if (stillRunning)
            return Oradpi_SetError(ip, (OradpiBase *)s, -1, "statement already executing asynchronously");
        AsyncRemove(s);
        ae = AsyncEnsure(s, NULL);
    }

    Tcl_MutexLock(&ae->lock);
    ae->owner      = s->owner;
    ae->doCommit   = commit;
    ae->autocommit = s->owner->autocommit;
    ae->rc         = 0;
    ae->done       = 0;
    ae->running    = 1;
    ae->canceled   = 0;
    ae->joined     = 0;
    ae->errorCode  = 0;
    if (ae->errorMsg) {
        Tcl_Free(ae->errorMsg);
        ae->errorMsg = NULL;
    }
    Tcl_MutexUnlock(&ae->lock);

    Tcl_Size    klen = 0;
    const char *kstr = Tcl_GetStringFromObj(objv[1], &klen);
    ae->stmtKey      = (char *)Tcl_Alloc((size_t)klen + 1);
    memcpy(ae->stmtKey, kstr, (size_t)klen);
    ae->stmtKey[klen] = '\0';

    if (dpiConn_addRef(s->owner->conn) != DPI_SUCCESS) {
        Tcl_Free(ae->stmtKey);
        ae->stmtKey = NULL;
        AsyncRemove(s);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiConn_addRef");
    }
    ae->conn = s->owner->conn;

    if (dpiStmt_addRef(s->stmt) != DPI_SUCCESS) {
        dpiConn_release(ae->conn);
        ae->conn = NULL;
        Tcl_Free(ae->stmtKey);
        ae->stmtKey = NULL;
        AsyncRemove(s);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_addRef");
    }
    ae->stmt            = s->stmt;

    OradpiAsyncArg *arg = (OradpiAsyncArg *)Tcl_Alloc(sizeof(*arg));
    arg->s              = s;
    int tcr             = Tcl_CreateThread(&ae->tid, AsyncWorker, (void *)arg, TCL_THREAD_STACK_DEFAULT, TCL_THREAD_NOFLAGS);
    if (tcr != TCL_OK) {
        Tcl_Free((char *)arg);
        dpiStmt_release(ae->stmt);
        ae->stmt = NULL;
        dpiConn_release(ae->conn);
        ae->conn = NULL;
        Tcl_Free(ae->stmtKey);
        ae->stmtKey = NULL;
        AsyncRemove(s);
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "failed to create async worker thread");
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_WaitAsync(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2 || objc > 4) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-timeout ms?");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int timeoutMs = -1;
    if (objc == 4) {
        const char *o = Tcl_GetString(objv[2]);
        if (strcmp(o, "-timeout") != 0) {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-timeout ms?");
            return TCL_ERROR;
        }
        if (Tcl_GetIntFromObj(ip, objv[3], &timeoutMs) != TCL_OK)
            return TCL_ERROR;
    }

    OradpiAsyncEntry *ae = AsyncLookup(s);
    if (!ae) {
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    int waited = 0;
    while (1) {
        int done, running;
        Tcl_MutexLock(&ae->lock);
        done    = ae->done;
        running = ae->running;
        Tcl_MutexUnlock(&ae->lock);
        if (done || !running)
            break;

        if (timeoutMs >= 0) {
            if (waited >= timeoutMs) {
                Oradpi_SetError(ip, (OradpiBase *)s, -3123, "asynchronous command still processing");
                Tcl_SetObjResult(ip, Tcl_NewIntObj(-3123));
                return TCL_OK;
            }
            Tcl_Sleep(10);
            waited += 10;
        } else {
            Tcl_Sleep(10);
        }
    }

    int   rc, errCode, iJoin;
    char *errMsg = NULL;
    Tcl_MutexLock(&ae->lock);
    rc      = ae->rc;
    errCode = ae->errorCode;
    if (ae->errorMsg) {
        size_t L = strlen(ae->errorMsg);
        errMsg   = (char *)Tcl_Alloc(L + 1);
        memcpy(errMsg, ae->errorMsg, L + 1);
    }
    iJoin = (ae->joined == 0);
    if (iJoin)
        ae->joined = 1;
    Tcl_MutexUnlock(&ae->lock);

    if (iJoin) {
        (void)Tcl_JoinThread(ae->tid, NULL);
        AsyncRemove(s);
    }

    const char *skey = Tcl_GetString(objv[1]);
    Oradpi_PendingsForget(ip, skey);

    Oradpi_UpdateStmtType(s);

    if (rc != 0) {
        if (errMsg) {
            Oradpi_SetError(ip, (OradpiBase *)s, errCode ? errCode : -1, errMsg);
            Tcl_Free(errMsg);
        } else {
            Oradpi_SetError(ip, (OradpiBase *)s, -1, "asynchronous execute failed");
        }
    }
    Tcl_SetObjResult(ip, Tcl_NewIntObj(rc));
    return TCL_OK;
}

int Oradpi_StmtWaitForAsync(OradpiStmt *s, int cancel, int timeoutMs) {
    OradpiAsyncEntry *ae = AsyncLookup(s);
    if (!ae)
        return 0;

    if (cancel && ae->conn) {
        (void)dpiConn_breakExecution(ae->conn);
        Tcl_MutexLock(&ae->lock);
        ae->canceled = 1;
        Tcl_MutexUnlock(&ae->lock);
    }

    int waited = 0;
    while (1) {
        int done, running;
        Tcl_MutexLock(&ae->lock);
        done    = ae->done;
        running = ae->running;
        Tcl_MutexUnlock(&ae->lock);
        if (done || !running)
            break;
        if (timeoutMs >= 0 && waited >= timeoutMs)
            return -3123;
        Tcl_Sleep(10);
        waited += 10;
    }

    int iJoin;
    Tcl_MutexLock(&ae->lock);
    iJoin = (ae->joined == 0);
    if (iJoin)
        ae->joined = 1;
    Tcl_MutexUnlock(&ae->lock);

    if (iJoin) {
        (void)Tcl_JoinThread(ae->tid, NULL);
        AsyncRemove(s);
    } else {
        while (AsyncLookup(s) != NULL) {
            Tcl_Sleep(5);
        }
    }
    return 0;
}

void Oradpi_CancelAndJoinAllForConn(Tcl_Interp *ip, OradpiConn *co) {
    if (!gAsyncInit || !co)
        return;
    Tcl_MutexLock(&gAsyncMutex);
    Tcl_HashSearch hs;
    Tcl_HashEntry *e;
    int            count = 0;
    for (e = Tcl_FirstHashEntry(&gAsyncByStmt, &hs); e; e = Tcl_NextHashEntry(&hs)) {
        OradpiAsyncEntry *ae = (OradpiAsyncEntry *)Tcl_GetHashValue(e);
        if (ae && ae->owner == co)
            count++;
    }
    OradpiStmt **keys     = (count > 0) ? (OradpiStmt **)Tcl_Alloc(sizeof(OradpiStmt *) * count) : NULL;
    char       **keyNames = (count > 0) ? (char **)Tcl_Alloc(sizeof(char *) * count) : NULL;

    int          idx      = 0;
    for (e = Tcl_FirstHashEntry(&gAsyncByStmt, &hs); e; e = Tcl_NextHashEntry(&hs)) {
        OradpiAsyncEntry *ae = (OradpiAsyncEntry *)Tcl_GetHashValue(e);
        if (ae && ae->owner == co) {
            keys[idx] = (OradpiStmt *)Tcl_GetHashKey(&gAsyncByStmt, e);
            if (ae->stmtKey) {
                size_t L      = strlen(ae->stmtKey);
                keyNames[idx] = (char *)Tcl_Alloc(L + 1);
                memcpy(keyNames[idx], ae->stmtKey, L + 1);
            } else {
                keyNames[idx] = NULL;
            }
            idx++;
        }
    }
    Tcl_MutexUnlock(&gAsyncMutex);

    for (int i = 0; i < count; i++) {
        (void)Oradpi_StmtWaitForAsync(keys[i], 1, -1);
        if (ip && keyNames[i]) {
            Oradpi_PendingsForget(ip, keyNames[i]);
            Tcl_Free(keyNames[i]);
        }
    }
    if (keys)
        Tcl_Free((char *)keys);
    if (keyNames)
        Tcl_Free((char *)keyNames);
}
