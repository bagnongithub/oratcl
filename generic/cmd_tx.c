/*
 *  cmd_tx.c --
 *
 *    Transaction control commands (commit/rollback) for ODPI connections.
 *
 *        - Affects only the target connection in the caller’s interpreter; no global mutable state.
 *        - Thread‑safe ODPI delegation with minimal locking.
 *
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include "cmd_int.h"
#include "dpi.h"
#include "state.h"

int Oradpi_Cmd_Commit(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    if (dpiConn_commit(co->conn) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_commit");
    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Rollback(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    if (dpiConn_rollback(co->conn) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_rollback");
    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
