/*
 *  oratcl_odpi.c --
 *
 *
 *    Package initialization and bootstrap for the Tcl 9 / ODPI-C bridge.
 *
 *    - Creates a single process-wide dpiContext guarded by a Tcl mutex (lazy init; exit handler).
 *    - Performs per-interpreter startup/teardown: allocates per-interp state, registers commands,
 *      and installs delete hooks so multiple interps can load the package safely (incl. static linking).
 *    - Ensures Oracle client libraries are loaded exactly once per process even when many interps init.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <string.h>
#include <tcl.h>

#include "cmd_int.h"
#include "dpi.h"

DLLEXPORT dpiContext *Oradpi_GlobalDpiContext = NULL;
static Tcl_Mutex     *gCtxMutex               = NULL;
static int            gExitHookRegistered     = 0;

static void           Oradpi_ProcessExit(void *unused) {
    (void)unused;
    if (!gCtxMutex)
        return;
    Tcl_MutexLock(gCtxMutex);
    if (Oradpi_GlobalDpiContext) {
        dpiContext_destroy(Oradpi_GlobalDpiContext);
        Oradpi_GlobalDpiContext = NULL;
    }
    Tcl_MutexUnlock(gCtxMutex);
}

int Oradpi_DpiContextEnsure(Tcl_Interp *ip) {
    if (!gCtxMutex)
        gCtxMutex = Tcl_GetAllocMutex();
    Tcl_MutexLock(gCtxMutex);
    if (!Oradpi_GlobalDpiContext) {
        dpiErrorInfo ei;
        if (dpiContext_create(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &Oradpi_GlobalDpiContext, &ei) != DPI_SUCCESS) {
            Tcl_MutexUnlock(gCtxMutex);
            if (ip) {
                Tcl_Obj *msg = Tcl_NewStringObj("oratcl: dpiContext_create failed: ", -1);
                if (ei.message && ei.messageLength > 0)
                    Tcl_AppendToObj(msg, ei.message, (int)ei.messageLength);
                Tcl_SetObjResult(ip, msg);
            }
            return TCL_ERROR;
        }
        if (!gExitHookRegistered) {
            Tcl_CreateExitHandler(Oradpi_ProcessExit, NULL);
            gExitHookRegistered = 1;
        }
    }
    Tcl_MutexUnlock(gCtxMutex);
    return TCL_OK;
}

static void RegisterCommands(Tcl_Interp *ip) {
    Tcl_CreateObjCommand2(ip, "oralogon", Oradpi_Cmd_Logon, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oralogoff", Oradpi_Cmd_Logoff, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraconfig", Oradpi_Cmd_Config, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orainfo", Oradpi_Cmd_Info, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraopen", Oradpi_Cmd_Open, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraclose", Oradpi_Cmd_Close, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orastmt", Oradpi_Cmd_Stmt, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraparse", Oradpi_Cmd_Parse, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orasql", Oradpi_Cmd_StmtSql, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orabind", Oradpi_Cmd_Orabind, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orabindexec", Oradpi_Cmd_Orabindexec, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraexec", Oradpi_Cmd_Exec, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraplexec", Oradpi_Cmd_Plexec, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orafetch", Oradpi_Cmd_Fetch, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oracols", Oradpi_Cmd_Cols, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oradesc", Oradpi_Cmd_Desc, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oramsg", Oradpi_Cmd_Msg, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oralob", Oradpi_Cmd_Lob, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraautocommit", Oradpi_Cmd_Autocommit, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraautocom", Oradpi_Cmd_Autocommit, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oracommit", Oradpi_Cmd_Commit, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraroll", Oradpi_Cmd_Rollback, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orarollback", Oradpi_Cmd_Rollback, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orabreak", Oradpi_Cmd_Break, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "oraexecasync", Oradpi_Cmd_ExecAsync, NULL, NULL);
    Tcl_CreateObjCommand2(ip, "orawaitasync", Oradpi_Cmd_WaitAsync, NULL, NULL);
}

#define ORATCL_INTERP_MARK "oradpi.loaded"

static void Oratcl_InterpDeleteProc(void *clientData, Tcl_Interp *ip) {
    (void)clientData;
    (void)Tcl_GetAssocData(ip, "oradpi.bindstore", NULL);
    (void)Tcl_GetAssocData(ip, "oradpi.pending", NULL);
    (void)Tcl_GetAssocData(ip, "oradpi", NULL);
}

DLLEXPORT int oratcl_Init(Tcl_Interp *ip) {
    if (Tcl_InitStubs(ip, "9.0-", 0) == NULL)
        return TCL_ERROR;
    if (Oradpi_DpiContextEnsure(ip) != TCL_OK)
        return TCL_ERROR;

    if (Tcl_GetAssocData(ip, ORATCL_INTERP_MARK, NULL) == NULL) {
        RegisterCommands(ip);
        Tcl_SetAssocData(ip, ORATCL_INTERP_MARK, Oratcl_InterpDeleteProc, (void *)1);
    }
    return Tcl_PkgProvide(ip, "oratcl", "9.0");
}

DLLEXPORT int oratcl_SafeInit(Tcl_Interp *ip) {
    return oratcl_Init(ip);
}
