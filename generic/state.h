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
#include <tcl.h>

#include "dpi.h"

#ifndef DPI_DEFAULT_FETCH_ARRAY_SIZE
#define DPI_DEFAULT_FETCH_ARRAY_SIZE 100
#endif

typedef struct OradpiMsg {
    int      rc;
    Tcl_Obj *fn;
    Tcl_Obj *sqlstate;
    Tcl_Obj *action;
    Tcl_Obj *error;
    uint64_t rows;
    int      sqltype;
    uint32_t peo;
    int      ocicode;
    int      recoverable;
    int      warning;
    uint32_t offset;
} OradpiMsg;

typedef struct OradpiBase {
    Tcl_Obj  *name;
    OradpiMsg msg;
} OradpiBase;

typedef struct OradpiConn {
    OradpiBase     base;
    dpiConn       *conn;
    dpiPool       *pool;
    int            autocommit;

    /* Connection-level configuration */
    uint32_t       stmtCacheSize;
    uint32_t       fetchArraySize;
    uint32_t       prefetchRows;
    uint32_t       prefetchMemory;
    uint32_t       callTimeout;
    int            inlineLobs;

    /* Driver-side failover policy (round-trippable) */
    uint32_t       foMaxAttempts;
    uint32_t       foBackoffMs;
    double         foBackoffFactor;
    uint32_t       foErrorClasses;
    uint32_t       foDebounceMs; /* debounce window for coalescing callbacks */

    /* Failover callback + dispatch context */
    Tcl_Obj       *failoverCallback;
    Tcl_Interp    *ownerIp;
    Tcl_ThreadId   ownerTid;

    /* Coalescing state */
    Tcl_TimerToken foTimer;
    int            foTimerScheduled;
    Tcl_Obj       *foPendingMsg;

    /* Cross-interp adoption control:
       - ownerClose: the interpreter that created the connection will perform dpiConn_close().
       - adopters only dpiConn_release() their addRef'ed handle. */
    int            ownerClose;
} OradpiConn;

typedef struct OradpiStmt {
    OradpiBase   base;
    OradpiConn  *owner;
    dpiStmt     *stmt;
    uint32_t     fetchArray;

    uint32_t     numCols;
    int          defined;

    Tcl_Mutex    asyncMutex;
    int          asyncRunning;
    int          asyncDone;
    int          asyncRc;
    uint32_t     asyncCols;
    Tcl_ThreadId asyncTid;
} OradpiStmt;

typedef struct OradpiLob {
    OradpiBase base;
    dpiLob    *lob;
} OradpiLob;

typedef struct OradpiInterpState {
    Tcl_Interp   *ip;
    Tcl_HashTable conns;
    Tcl_HashTable stmts;
    Tcl_HashTable lobs;
} OradpiInterpState;

OradpiLob *Oradpi_LookupLob(Tcl_Interp *ip, Tcl_Obj *nameObj);

#endif /* ORATCL_ODPI_STATE_H */
