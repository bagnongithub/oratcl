/*
 *  cmd_msg.c --
 *
 *    Message and status reporting utilities.
 *
 *        - Returns last error and rows-affected from per-handle message areas, preserving Oratcl 4.6 options.
 *        - Read-only queries of per-interp state; safe in multi-threaded scenarios.
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

static OradpiBase* Lookup(Tcl_Interp* ip, Tcl_Obj* h);
int Oradpi_Cmd_Msg(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

static OradpiBase* Lookup(Tcl_Interp* ip, Tcl_Obj* h)
{
    OradpiBase* b = NULL;
    OradpiConn* co = Oradpi_LookupConn(ip, h);
    if (co)
        b = (OradpiBase*)co;
    else
    {
        OradpiStmt* s = Oradpi_LookupStmt(ip, h);
        if (s)
            b = (OradpiBase*)s;
    }
    return b;
}

static const char* const msgOptionNames[] = {"rc",
                                             "error",
                                             "rows",
                                             "peo",
                                             "ocicode",
                                             "sqltype",
                                             "fn",
                                             "action",
                                             "sqlstate",
                                             "recoverable",
                                             "warning",
                                             "offset",
                                             "all",
                                             "allx",
                                             NULL};
enum MsgOptionIdx
{
    MSG_RC,
    MSG_ERROR,
    MSG_ROWS,
    MSG_PEO,
    MSG_OCICODE,
    MSG_SQLTYPE,
    MSG_FN,
    MSG_ACTION,
    MSG_SQLSTATE,
    MSG_RECOVERABLE,
    MSG_WARNING,
    MSG_OFFSET,
    MSG_ALL,
    MSG_ALLX
};

/*
 * oramsg handle option
 *
 *   Queries the per-handle message area for the last operation's status.
 *   Options: rc, error, rows, peo, ocicode, sqltype, fn, action, sqlstate,
 *   recoverable, warning, offset, all, allx.
 *   Returns: the requested value or a key-value list (all/allx).
 *   Errors:  invalid handle; unknown option.
 *   Thread-safety: safe — read-only access to per-interp handle state.
 */
int Oradpi_Cmd_Msg(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "handle option");
        return TCL_ERROR;
    }
    OradpiBase* b = Lookup(ip, objv[1]);
    if (!b)
        return Oradpi_SetError(ip, NULL, -1, "invalid handle");

    int idx;
    if (Tcl_GetIndexFromObj(ip, objv[2], msgOptionNames, "option", 0, &idx) != TCL_OK)
        return TCL_ERROR;

    switch ((enum MsgOptionIdx)idx)
    {
        case MSG_RC:
            Tcl_SetObjResult(ip, Tcl_NewIntObj(b->msg.rc));
            return TCL_OK;
        case MSG_ERROR:
            Tcl_SetObjResult(ip, b->msg.error ? Oradpi_SnapshotObj(b->msg.error) : Tcl_NewObj());
            return TCL_OK;
        case MSG_ROWS:
            Tcl_SetObjResult(ip, Tcl_NewWideIntObj((Tcl_WideInt)b->msg.rows));
            return TCL_OK;
        case MSG_PEO:
            Tcl_SetObjResult(ip, Tcl_NewWideIntObj((Tcl_WideInt)(uint64_t)b->msg.peo));
            return TCL_OK;
        case MSG_OCICODE:
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)b->msg.ocicode));
            return TCL_OK;
        case MSG_SQLTYPE:
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)b->msg.sqltype));
            return TCL_OK;
        case MSG_FN:
            Tcl_SetObjResult(ip, b->msg.fn ? Oradpi_SnapshotObj(b->msg.fn) : Tcl_NewObj());
            return TCL_OK;
        case MSG_ACTION:
            Tcl_SetObjResult(ip, b->msg.action ? Oradpi_SnapshotObj(b->msg.action) : Tcl_NewObj());
            return TCL_OK;
        case MSG_SQLSTATE:
            Tcl_SetObjResult(ip, b->msg.sqlstate ? Oradpi_SnapshotObj(b->msg.sqlstate) : Tcl_NewObj());
            return TCL_OK;
        case MSG_RECOVERABLE:
            Tcl_SetObjResult(ip, Tcl_NewBooleanObj(b->msg.recoverable));
            return TCL_OK;
        case MSG_WARNING:
            Tcl_SetObjResult(ip, Tcl_NewBooleanObj(b->msg.warning));
            return TCL_OK;
        case MSG_OFFSET:
            Tcl_SetObjResult(ip, Tcl_NewWideIntObj((Tcl_WideInt)(uint64_t)b->msg.offset));
            return TCL_OK;
        case MSG_ALL:
        case MSG_ALLX:
        {
            Tcl_Obj* res = Tcl_NewListObj(0, NULL);
            LAPPEND_CHK(ip, res, Tcl_NewStringObj("rc", -1));
            LAPPEND_CHK(ip, res, Tcl_NewIntObj(b->msg.rc));
            LAPPEND_CHK(ip, res, Tcl_NewStringObj("error", -1));
            LAPPEND_CHK(ip, res, b->msg.error ? Oradpi_SnapshotObj(b->msg.error) : Tcl_NewObj());
            LAPPEND_CHK(ip, res, Tcl_NewStringObj("rows", -1));
            LAPPEND_CHK(ip, res, Tcl_NewWideIntObj((Tcl_WideInt)b->msg.rows));
            LAPPEND_CHK(ip, res, Tcl_NewStringObj("peo", -1));
            LAPPEND_CHK(ip, res, Tcl_NewWideIntObj((Tcl_WideInt)(uint64_t)b->msg.peo));
            LAPPEND_CHK(ip, res, Tcl_NewStringObj("ocicode", -1));
            LAPPEND_CHK(ip, res, Tcl_NewIntObj((int)b->msg.ocicode));
            LAPPEND_CHK(ip, res, Tcl_NewStringObj("sqltype", -1));
            LAPPEND_CHK(ip, res, Tcl_NewIntObj((int)b->msg.sqltype));
            if (idx == MSG_ALLX)
            {
                LAPPEND_CHK(ip, res, Tcl_NewStringObj("fn", -1));
                LAPPEND_CHK(ip, res, b->msg.fn ? Oradpi_SnapshotObj(b->msg.fn) : Tcl_NewObj());
                LAPPEND_CHK(ip, res, Tcl_NewStringObj("action", -1));
                LAPPEND_CHK(ip, res, b->msg.action ? Oradpi_SnapshotObj(b->msg.action) : Tcl_NewObj());
                LAPPEND_CHK(ip, res, Tcl_NewStringObj("sqlstate", -1));
                LAPPEND_CHK(ip, res, b->msg.sqlstate ? Oradpi_SnapshotObj(b->msg.sqlstate) : Tcl_NewObj());
                LAPPEND_CHK(ip, res, Tcl_NewStringObj("recoverable", -1));
                LAPPEND_CHK(ip, res, Tcl_NewBooleanObj(b->msg.recoverable));
                LAPPEND_CHK(ip, res, Tcl_NewStringObj("warning", -1));
                LAPPEND_CHK(ip, res, Tcl_NewBooleanObj(b->msg.warning));
                LAPPEND_CHK(ip, res, Tcl_NewStringObj("offset", -1));
                LAPPEND_CHK(ip, res, Tcl_NewWideIntObj((Tcl_WideInt)(uint64_t)b->msg.offset));
            }
            Tcl_SetObjResult(ip, res);
            return TCL_OK;
        }
    }
    /* unreachable */
    return TCL_ERROR;
}
