/*
 *  cmd_msg.c --
 *
 *    Message and status reporting utilities.
 *
 *        - Returns last error and rows‑affected from per‑handle message areas, preserving Oratcl 4.6 options.
 *        - Read‑only queries of per‑interp state; safe in multi‑threaded scenarios.
 *
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <string.h>

#include "cmd_int.h"

static OradpiBase *Lookup(Tcl_Interp *ip, Tcl_Obj *h) {
    OradpiBase *b  = NULL;
    OradpiConn *co = Oradpi_LookupConn(ip, h);
    if (co)
        b = (OradpiBase *)co;
    else {
        OradpiStmt *s = Oradpi_LookupStmt(ip, h);
        if (s)
            b = (OradpiBase *)s;
    }
    return b;
}

int Oradpi_Cmd_Msg(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    if (objc < 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "handle option");
        return TCL_ERROR;
    }
    OradpiBase *b = Lookup(ip, objv[1]);
    if (!b)
        return Oradpi_SetError(ip, NULL, -1, "invalid handle");
    const char *opt = Tcl_GetString(objv[2]);
    if (strcmp(opt, "rc") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewIntObj(b->msg.rc));
        return TCL_OK;
    }
    if (strcmp(opt, "error") == 0) {
        Tcl_SetObjResult(ip, b->msg.error ? b->msg.error : Tcl_NewObj());
        return TCL_OK;
    }
    if (strcmp(opt, "rows") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewWideIntObj((Tcl_WideInt)b->msg.rows));
        return TCL_OK;
    }
    if (strcmp(opt, "peo") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewIntObj((int)b->msg.peo));
        return TCL_OK;
    }
    if (strcmp(opt, "ocicode") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewIntObj((int)b->msg.ocicode));
        return TCL_OK;
    }
    if (strcmp(opt, "sqltype") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewIntObj((int)b->msg.sqltype));
        return TCL_OK;
    }
    if (strcmp(opt, "fn") == 0) {
        Tcl_SetObjResult(ip, b->msg.fn ? b->msg.fn : Tcl_NewObj());
        return TCL_OK;
    }
    if (strcmp(opt, "action") == 0) {
        Tcl_SetObjResult(ip, b->msg.action ? b->msg.action : Tcl_NewObj());
        return TCL_OK;
    }
    if (strcmp(opt, "sqlstate") == 0) {
        Tcl_SetObjResult(ip, b->msg.sqlstate ? b->msg.sqlstate : Tcl_NewObj());
        return TCL_OK;
    }
    if (strcmp(opt, "recoverable") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewBooleanObj(b->msg.recoverable));
        return TCL_OK;
    }
    if (strcmp(opt, "warning") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewBooleanObj(b->msg.warning));
        return TCL_OK;
    }
    if (strcmp(opt, "offset") == 0) {
        Tcl_SetObjResult(ip, Tcl_NewIntObj((int)b->msg.offset));
        return TCL_OK;
    }
    if (strcmp(opt, "all") == 0 || strcmp(opt, "allx") == 0) {
        Tcl_Obj *res = Tcl_NewListObj(0, NULL);
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("rc", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj(b->msg.rc));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("error", -1));
        Tcl_ListObjAppendElement(ip, res, b->msg.error ? b->msg.error : Tcl_NewObj());
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("rows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewWideIntObj((Tcl_WideInt)b->msg.rows));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("peo", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)b->msg.peo));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("ocicode", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)b->msg.ocicode));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("sqltype", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)b->msg.sqltype));
        if (strcmp(opt, "allx") == 0) {
            Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("fn", -1));
            Tcl_ListObjAppendElement(ip, res, b->msg.fn ? b->msg.fn : Tcl_NewObj());
            Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("action", -1));
            Tcl_ListObjAppendElement(ip, res, b->msg.action ? b->msg.action : Tcl_NewObj());
            Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("sqlstate", -1));
            Tcl_ListObjAppendElement(ip, res, b->msg.sqlstate ? b->msg.sqlstate : Tcl_NewObj());
            Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("recoverable", -1));
            Tcl_ListObjAppendElement(ip, res, Tcl_NewBooleanObj(b->msg.recoverable));
            Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("warning", -1));
            Tcl_ListObjAppendElement(ip, res, Tcl_NewBooleanObj(b->msg.warning));
            Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("offset", -1));
            Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)b->msg.offset));
        }
        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }
    return Oradpi_SetError(ip, b, -1, "unknown option");
}
