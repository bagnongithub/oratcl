/*
 *  cmd_int.h --
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#ifndef ORATCL_ODPI_CMD_INT_H
#define ORATCL_ODPI_CMD_INT_H

#include <stddef.h>
#include <stdint.h>
#ifndef USE_TCL_STUBS
#define USE_TCL_STUBS
#endif
#include <tcl.h>

#include "dpi.h"
#include "state.h"

/* =========================================================================
 * S-4: Global mutex lock ordering
 *
 * All Tcl_Mutex instances in this extension and their ordering constraints.
 * A thread holding lock N must never attempt to acquire lock N-1 or earlier.
 *
 *   1. gCtxMutex        (oratcl_odpi.c) — protects Oradpi_GlobalDpiContext.
 *                        Acquire before any other module mutex.
 *   2. gAsyncMutex      (async.c)       — protects gAsyncByKey registry.
 *                        Acquire before any per-entry ae->lock.
 *   3. ae->lock          (async.c)       — per-entry async state.
 *                        Leaf within the async subsystem.
 *   4. gPoolInitMutex   (async.c)       — protects gPool struct.
 *                        Leaf lock (no other locks held).
 *   5. gPool.queueMutex (async.c)       — protects thread pool work queue.
 *                        Leaf lock (no other locks held).
 *   6. gConnMapMutex    (state.c)       — protects gConnByName hash table.
 *                        Leaf lock.
 *   7. gHandleMutex     (util.c)        — protects handle name counter.
 *                        Leaf lock.
 *   8. gTypeInitMutex   (cmd_bind.c)    — protects gBytearrayType one-time init.
 *                        Leaf lock.
 *   9. GlobalConnRec.connLock (state.c) — shared per-dpiConn operation gate.
 *                        Serializes ODPI/OCI calls across owner, adopters,
 *                        and async workers for the same dpiConn*.
 *                        Leaf lock; never hold while acquiring gAsyncMutex
 *                        or gConnMapMutex.
 *
 * Leaf locks (4-9) are independent and may be acquired in any order
 * relative to each other, but never while holding a non-leaf lock (1-3)
 * if the non-leaf lock's critical section could call into those modules.
 *
 * S-5: Worker thread contract — NO Tcl_Interp API in worker threads
 *
 * Pool worker threads (async.c) MUST NOT call any Tcl API that requires
 * a Tcl_Interp* or operates on Tcl_Obj* values.  Workers may only use:
 *   - Tcl_Mutex / Tcl_Condition (synchronization)
 *   - Tcl_Alloc / Tcl_Free / Tcl_Realloc (memory)
 *   - Tcl_Sleep (backoff)
 *   - Tcl_GetCurrentThread (diagnostic)
 *   - ODPI-C APIs on their own addRef'd dpiStmt / dpiConn handles
 * If future features need to report results from a worker to an interp,
 * use Tcl_ThreadQueueEvent + Tcl_ThreadAlert with deep-copied payloads.
 *
 * S-3: Design note — Tcl_GetWideIntFromObj vs Tcl_GetSizeIntFromObj
 *
 * ODPI-C APIs uniformly use uint32_t for counts and sizes. This extension
 * deliberately uses Tcl_GetWideIntFromObj + explicit range checks to
 * uint32_t rather than Tcl_GetSizeIntFromObj, because the target type
 * is ODPI's uint32_t (not Tcl_Size), and the manual checks provide
 * clear error messages identifying the specific overflow.
 * ========================================================================= */

int Oradpi_Cmd_Autocommit(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Break(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Close(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Cols(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Commit(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Config(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Desc(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Exec(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_ExecAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Fetch(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Info(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Lob(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Logoff(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Logon(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Msg(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Open(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Orabind(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Orabindexec(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Parse(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Plexec(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Rollback(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Stmt(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_StmtSql(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_WaitAsync(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);

/* Util */
Tcl_Obj* Oradpi_NewHandleName(Tcl_Interp* ip, const char* prefix);
int Oradpi_SetError(Tcl_Interp* ip, OradpiBase* h, int rc, const char* msg);
int Oradpi_SetErrorFromODPI(Tcl_Interp* ip, OradpiBase* h, const char* fn);
int Oradpi_SetErrorFromODPIInfo(Tcl_Interp* ip, OradpiBase* h, const char* where, const dpiErrorInfo* ei);
void Oradpi_RecordRows(OradpiBase* h, uint64_t rows);
void Oradpi_UpdateStmtType(OradpiStmt* s);
void Oradpi_FreeMsg(OradpiMsg* m);

extern dpiContext* Oradpi_GlobalDpiContext;
dpiContext* Oradpi_GetDpiContext(void);
int Oradpi_CaptureODPIError(dpiErrorInfo* ei);

#define ORATCL_NAMESPACE "::oratcl"

/* V-8: Keep in sync with AC_INIT version in configure.ac */
#define ORATCL_VERSION "9.0"
#define ORATCL_TCL_MIN "9"

#define ORADPI_FO_CLASS_NETWORK 0x01u
#define ORADPI_FO_CLASS_CONNLOST 0x02u

#define ORA_ERR_BROKEN_PIPE 3113
#define ORA_ERR_NOT_CONNECTED 3114
#define ORA_ERR_LOST_CONTACT 3135
#define ORA_ERR_TNS_LOST_CONTACT 12547

static inline Tcl_Obj* Oradpi_NewUInt32Obj(uint32_t value)
{
    return Tcl_NewWideIntObj((Tcl_WideInt)(uint64_t)value);
}

static inline Tcl_Obj* Oradpi_SnapshotObj(Tcl_Obj* obj)
{
    return obj ? Tcl_DuplicateObj(obj) : Tcl_NewObj();
}

static inline int Oradpi_CheckedTclSizeToSizeT(Tcl_Interp* ip, Tcl_Size len, size_t* out, const char* what)
{
    if (len < 0)
    {
        if (ip)
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s is negative", what ? what : "size"));
        return TCL_ERROR;
    }
    if ((uint64_t)len > (uint64_t)((size_t)-1))
    {
        if (ip)
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s exceeds size_t range", what ? what : "size"));
        return TCL_ERROR;
    }
    *out = (size_t)len;
    return TCL_OK;
}

static inline int Oradpi_CheckedU64ToSizeT(Tcl_Interp* ip, uint64_t len, size_t* out, const char* what)
{
    if (len > (uint64_t)((size_t)-1))
    {
        if (ip)
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s exceeds size_t range", what ? what : "size"));
        return TCL_ERROR;
    }
    *out = (size_t)len;
    return TCL_OK;
}

static inline int Oradpi_CheckedWideIntToU32(Tcl_Interp* ip, Tcl_WideInt value, uint32_t* out, const char* what)
{
    if (value < 0)
    {
        if (ip)
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s must be >= 0", what ? what : "value"));
        return TCL_ERROR;
    }
    if ((uint64_t)value > UINT32_MAX)
    {
        if (ip)
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s exceeds uint32 range", what ? what : "value"));
        return TCL_ERROR;
    }
    *out = (uint32_t)value;
    return TCL_OK;
}

static inline int Oradpi_GetUInt32FromObj(Tcl_Interp* ip, Tcl_Obj* obj, uint32_t* out, const char* what)
{
    Tcl_WideInt value = 0;
    if (Tcl_GetWideIntFromObj(ip, obj, &value) != TCL_OK)
        return TCL_ERROR;
    return Oradpi_CheckedWideIntToU32(ip, value, out, what);
}

static inline int Oradpi_CheckedAllocBytes(Tcl_Interp* ip, Tcl_Size count, size_t elemSize, size_t* out, const char* what)
{
    size_t countSize = 0;
    if (Oradpi_CheckedTclSizeToSizeT(ip, count, &countSize, what) != TCL_OK)
        return TCL_ERROR;
    if (elemSize != 0 && countSize > ((size_t)-1) / elemSize)
    {
        if (ip)
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("%s allocation overflows size_t", what ? what : "buffer"));
        return TCL_ERROR;
    }
    *out = countSize * elemSize;
    return TCL_OK;
}

/* C-1 fix: Finite timeout (ms) for teardown waits.  30 seconds gives the
 * Oracle server a generous window to respond to dpiConn_breakExecution
 * before we give up and let the worker self-clean via the orphan mechanism. */
#ifndef ORADPI_TEARDOWN_TIMEOUT_MS
#define ORADPI_TEARDOWN_TIMEOUT_MS 30000
#endif

#define CONN_GATE_ENTER(co) Oradpi_ConnGateEnter((co))
#define CONN_GATE_ENTER_TIMED(co, timeoutMs) Oradpi_ConnGateEnterTimed((co), (timeoutMs))
#define CONN_GATE_LEAVE(co) Oradpi_ConnGateLeave((co))
#define CONN_BREAK(co) Oradpi_ConnBreak((co))

/* V-6 fix: centralized checked list-append macro.  Every call to
 * Tcl_ListObjAppendElement() must inspect its return code.  Duplicated
 * from cmd_msg.c so all translation units can use it. */
#define LAPPEND_CHK(ip, list, obj)                                                                                               \
    do                                                                                                                           \
    {                                                                                                                            \
        if (Tcl_ListObjAppendElement((ip), (list), (obj)) != TCL_OK)                                                             \
            return TCL_ERROR;                                                                                                    \
    } while (0)

/* V-8 fix: Cleanup-compatible variant — jumps to a label instead of
 * returning, for use in functions that need resource cleanup on error
 * (cmd_desc.c, cmd_fetch.c, util.c). Sets code=TCL_ERROR before goto. */
#define LAPPEND_GOTO(ip, list, obj, errcode, label)                                                                              \
    do                                                                                                                           \
    {                                                                                                                            \
        if (Tcl_ListObjAppendElement((ip), (list), (obj)) != TCL_OK)                                                             \
        {                                                                                                                        \
            (errcode) = TCL_ERROR;                                                                                               \
            goto label;                                                                                                          \
        }                                                                                                                        \
    } while (0)

/* State ops */
OradpiInterpState* Oradpi_GetInterpState(Tcl_Interp* ip);
OradpiConn* Oradpi_NewConn(Tcl_Interp* ip, dpiConn* conn, dpiPool* pool);
OradpiConn* Oradpi_LookupConn(Tcl_Interp* ip, Tcl_Obj* nameObj);
OradpiStmt* Oradpi_NewStmt(Tcl_Interp* ip, OradpiConn* co);
OradpiStmt* Oradpi_LookupStmt(Tcl_Interp* ip, Tcl_Obj* nameObj);
OradpiLob* Oradpi_NewLob(Tcl_Interp* ip, dpiLob* lob, GlobalConnRec* shared);
void Oradpi_FreeConn(OradpiConn* co);
void Oradpi_FreeStmt(Tcl_Interp* ip, OradpiStmt* s);
void Oradpi_FreeLob(OradpiLob* l);
void Oradpi_DeleteInterpData(void* clientData, Tcl_Interp* ip);
void Oradpi_RemoveStmt(Tcl_Interp* ip, OradpiStmt* s);
void Oradpi_RemoveLob(Tcl_Interp* ip, OradpiLob* l);

int Oradpi_DpiContextEnsure(Tcl_Interp* ip);

/* Async APIs */
int Oradpi_StmtWaitForAsync(OradpiStmt* s, int doCancel, int timeoutMs);
int Oradpi_StmtIsAsyncBusy(OradpiStmt* s);
void Oradpi_CancelAndJoinAllForConn(Tcl_Interp* ip, OradpiConn* co);

/* Shared bind infrastructure (cmd_bind.c) */
typedef struct OradpiPendingRefs
{
    Tcl_Size n, cap;
    dpiVar** vars;
} OradpiPendingRefs;

void Oradpi_PendingsInit(OradpiPendingRefs* pr);
void Oradpi_PendingsAdd(OradpiPendingRefs* pr, dpiVar* v);
void Oradpi_PendingsReleaseAll(OradpiPendingRefs* pr);
void Oradpi_PendingsFree(OradpiPendingRefs* pr);

/* FIX 4 (MAJOR): clear helpers for the embedded maps in OradpiInterpState */
void Oradpi_ClearBindStoreMap(BindStoreMap* bm);
void Oradpi_ClearPendingMap(PendingMap* pm);

void Oradpi_BindStoreForget(Tcl_Interp* ip, const char* stmtKey);
void Oradpi_PendingsForget(Tcl_Interp* ip, const char* stmtKey);
void Oradpi_ClearBindStoreForStmt(Tcl_Interp* ip, const char* stmtKey);
int Oradpi_BindOneByValue(Tcl_Interp* ip, OradpiStmt* s, OradpiPendingRefs* pr, const char* nameNoColon, Tcl_Obj* valueObj);
int Oradpi_RebindAllStored(Tcl_Interp* ip, OradpiStmt* s, OradpiPendingRefs* pr, const char* stmtKey);
uint32_t Oradpi_WithColon(const char* nameNoColon, char* dst, uint32_t cap);
const char* Oradpi_StripColon(const char* raw);

#endif /* ORATCL_ODPI_CMD_INT_H */
