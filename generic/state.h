/*
 *  state.h --
 *
 *    Shared type and state definitions for the extension.
 *
 *        - Declares handle structs for connections, statements, LOBs, and the per‑interpreter state block.
 *        - Designed for multi‑interp/multi‑thread use: per‑interp registries and reference tracking ensure
 *          safe teardown; process‑wide data is protected by Tcl mutexes.
 *
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#ifndef ORATCL_ODPI_STATE_H
#define ORATCL_ODPI_STATE_H

#include <stdint.h>
#ifndef USE_TCL_STUBS
#define USE_TCL_STUBS
#endif
#include <tcl.h>

#include "dpi.h"

#ifndef DPI_DEFAULT_FETCH_ARRAY_SIZE
#define DPI_DEFAULT_FETCH_ARRAY_SIZE 100
#endif

/* M-2: Guard against ODPI-C versions that don't define this macro */
#ifndef DPI_DEFAULT_PREFETCH_ROWS
#define DPI_DEFAULT_PREFETCH_ROWS 2
#endif

#ifndef BINDSTORE_ASSOC
#define BINDSTORE_ASSOC "oradpi.bindstore"
#endif

#ifndef PENDING_ASSOC
#define PENDING_ASSOC "oradpi.pending"
#endif

typedef struct GlobalConnRec GlobalConnRec;

typedef struct OradpiMsg
{
    int rc;
    Tcl_Obj* fn;
    Tcl_Obj* sqlstate;
    Tcl_Obj* action;
    Tcl_Obj* error;
    uint64_t rows;
    int sqltype;
    uint32_t peo;
    int ocicode;
    int recoverable;
    int warning;
    uint32_t offset;
} OradpiMsg;

typedef struct OradpiBase
{
    Tcl_Obj* name;
    OradpiMsg msg;
} OradpiBase;

typedef struct OradpiConn
{
    OradpiBase base;
    dpiConn* conn;
    dpiPool* pool;
    int autocommit;

    /* Connection-level configuration */
    uint32_t stmtCacheSize;
    uint32_t fetchArraySize;
    uint32_t prefetchRows;
    uint32_t callTimeout;
    int inlineLobs;

    /* Cached encoding string from ODPI (avoids per-bind round-trip) */
    char* cachedEncoding;

    /* Driver-side failover policy (round-trippable) */
    uint32_t foMaxAttempts;
    uint32_t foBackoffMs;
    double foBackoffFactor;
    uint32_t foErrorClasses;
    uint32_t foDebounceMs; /* debounce window for coalescing callbacks */

    /* Failover callback + dispatch context */
    Tcl_Obj* failoverCallback;
    Tcl_Interp* ownerIp;
    Tcl_ThreadId ownerTid;

    /* Coalescing state */
    Tcl_TimerToken foTimer;
    int foTimerScheduled;
    Tcl_Obj* foPendingMsg;

    /* Cross-interp adoption control:
       - ownerClose: the interpreter that created the connection performs
         dpiConn_close(); adopters only dpiConn_release() their addRef'ed
         handle.
       - shared: refcounted process-global adoption record for this dpiConn*.
         It owns the shared per-connection gate used to serialize ODPI calls
         across the owner wrapper, adopted wrappers, and async workers.
       - Callers must use Oradpi_ConnGateEnter/Leave (or the CONN_GATE_* macros)
         instead of locking any per-wrapper field directly. */
    int ownerClose;
    GlobalConnRec* shared; /* shared per-dpiConn gate and adoption record */
    int adopted;           /* nonzero if this handle was adopted from another interp */
} OradpiConn;

typedef struct OradpiStmt
{
    OradpiBase base;
    OradpiConn* owner;
    dpiStmt* stmt;
    uint32_t fetchArray;
    uint32_t prefetchRows; /* per-statement override; 0 = use connection default */

    uint32_t numCols;
    int defined;
} OradpiStmt;

typedef struct OradpiLob
{
    OradpiBase base;
    dpiLob* lob;
    GlobalConnRec* shared; /* shared per-dpiConn gate for serializing LOB I/O */
} OradpiLob;

typedef struct OradpiInterpState
{
    Tcl_Interp* ip;
    Tcl_HashTable conns;
    Tcl_HashTable stmts;
    Tcl_HashTable lobs;
} OradpiInterpState;

OradpiLob* Oradpi_LookupLob(Tcl_Interp* ip, Tcl_Obj* nameObj);

/* Shared adopted-connection gate APIs. */
void Oradpi_ConnGateEnter(OradpiConn* co);
int Oradpi_ConnGateEnterTimed(OradpiConn* co, int timeoutMs);
void Oradpi_ConnGateLeave(OradpiConn* co);
void Oradpi_ConnBreak(OradpiConn* co);

void Oradpi_SharedConnAddRef(GlobalConnRec* gr);
void Oradpi_SharedConnRelease(GlobalConnRec* gr);
void Oradpi_SharedConnGateEnter(GlobalConnRec* gr);
int Oradpi_SharedConnGateEnterTimed(GlobalConnRec* gr, int timeoutMs);
void Oradpi_SharedConnGateLeave(GlobalConnRec* gr);
void Oradpi_SharedConnBreak(GlobalConnRec* gr, dpiConn* conn);
void Oradpi_SharedConnSyncBehavior(OradpiConn* co);
void* Oradpi_ConnGateToken(OradpiConn* co);

#endif /* ORATCL_ODPI_STATE_H */
