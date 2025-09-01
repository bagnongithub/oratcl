/*
 *  cmd_int.h --
 *
 *    LOB handle operations (size/read/write/trim/close).
 *
 *        - Thin wrappers over ODPI LOB APIs with Oratcl handle naming semantics.
 *        - Worker‑thread friendly: I/O paths hold strong references to underlying ODPI handles.
 *
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#ifndef ORATCL_ODPI_CMD_INT_H
#define ORATCL_ODPI_CMD_INT_H

#include <stdint.h>
#include <tcl.h>

#include "dpi.h"
#include "state.h"

int         Oradpi_Cmd_Autocommit(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Break(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Close(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Cols(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Commit(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Config(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Desc(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Exec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_ExecAsync(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Fetch(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Info(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Lob(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Logoff(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Logon(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Msg(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Open(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Orabind(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Orabindexec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Parse(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Plexec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Rollback(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_Stmt(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_StmtSql(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
int         Oradpi_Cmd_WaitAsync(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);

/* Util */
Tcl_Obj    *Oradpi_NewHandleName(Tcl_Interp *ip, const char *prefix);
int         Oradpi_SetError(Tcl_Interp *ip, OradpiBase *h, int rc, const char *msg);
int         Oradpi_SetErrorFromODPI(Tcl_Interp *ip, OradpiBase *h, const char *fn);
int         Oradpi_IsNumberObj(Tcl_Obj *o, long long *asInt, double *asDbl, int *isInt);
void        Oradpi_RecordRows(OradpiBase *h, uint64_t rows);
void        Oradpi_UpdateStmtType(OradpiStmt *s);

/* State ops */
OradpiConn *Oradpi_NewConn(Tcl_Interp *ip, dpiConn *conn, dpiPool *pool);
OradpiConn *Oradpi_LookupConn(Tcl_Interp *ip, Tcl_Obj *nameObj);
OradpiStmt *Oradpi_NewStmt(Tcl_Interp *ip, OradpiConn *co);
OradpiStmt *Oradpi_LookupStmt(Tcl_Interp *ip, Tcl_Obj *nameObj);
OradpiLob  *Oradpi_NewLob(Tcl_Interp *ip, dpiLob *lob);
void        Oradpi_DeleteInterpData(void *clientData, Tcl_Interp *ip);

int         Oradpi_DpiContextEnsure(Tcl_Interp *ip);

/* Async APIs */
int         Oradpi_StmtWaitForAsync(OradpiStmt *s, int doCancel, int timeoutMs);
void        Oradpi_CancelAndJoinAllForConn(Tcl_Interp *ip, OradpiConn *co);

#endif /* ORATCL_ODPI_CMD_INT_H */
