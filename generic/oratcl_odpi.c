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
#ifndef USE_TCL_STUBS
#define USE_TCL_STUBS
#endif
#include <tcl.h>

#include "cmd_int.h"
#include "dpi.h"

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

int Oradpi_DpiContextEnsure(Tcl_Interp* ip);
static void Oradpi_ProcessExit(void* unused);
static void Oratcl_InterpDeleteProc(void* clientData, Tcl_Interp* ip);
static void RegisterCommands(Tcl_Interp* ip);
DLLEXPORT int oratcl_Init(Tcl_Interp* ip);
DLLEXPORT int oratcl_SafeInit(Tcl_Interp* ip);

/* ------------------------------------------------------------------------- *
 * Process-global Tcl/ODPI state
 * ------------------------------------------------------------------------- */

dpiContext* Oradpi_GlobalDpiContext = NULL;
/* gCtxMutex: protects Oradpi_GlobalDpiContext and gExitHookRegistered.
 * Lock ordering: acquire gCtxMutex before any other module mutex. */
static Tcl_Mutex gCtxMutex; /* zero-initialized; auto-created by Tcl on first lock */
static int gExitHookRegistered = 0;

static void Oradpi_ProcessExit(void* unused)
{
    (void)unused;
    Tcl_MutexLock(&gCtxMutex);
    if (Oradpi_GlobalDpiContext)
    {
        dpiContext_destroy(Oradpi_GlobalDpiContext);
        Oradpi_GlobalDpiContext = NULL;
    }
    Tcl_MutexUnlock(&gCtxMutex);
}

dpiContext* Oradpi_GetDpiContext(void)
{
    dpiContext* ctx = NULL;
    Tcl_MutexLock(&gCtxMutex);
    ctx = Oradpi_GlobalDpiContext;
    Tcl_MutexUnlock(&gCtxMutex);
    return ctx;
}

int Oradpi_CaptureODPIError(dpiErrorInfo* ei)
{
    dpiContext* ctx = Oradpi_GetDpiContext();
    if (!ctx || !ei)
        return 0;
    memset(ei, 0, sizeof(*ei));
    dpiContext_getError(ctx, ei);
    return 1;
}

int Oradpi_DpiContextEnsure(Tcl_Interp* ip)
{
    Tcl_MutexLock(&gCtxMutex);
    if (!Oradpi_GlobalDpiContext)
    {
        dpiErrorInfo ei;
        if (dpiContext_create(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &Oradpi_GlobalDpiContext, &ei) != DPI_SUCCESS)
        {
            Tcl_MutexUnlock(&gCtxMutex);
            if (ip)
            {
                Tcl_Obj* msg = Tcl_NewStringObj("oratcl: dpiContext_create failed: ", -1);
                if (ei.message && ei.messageLength > 0)
                    Tcl_AppendToObj(msg, ei.message, (Tcl_Size)ei.messageLength);
                Tcl_SetObjResult(ip, msg);
            }
            return TCL_ERROR;
        }
        if (!gExitHookRegistered)
        {
            Tcl_CreateExitHandler(Oradpi_ProcessExit, NULL);
            gExitHookRegistered = 1;
        }
    }
    Tcl_MutexUnlock(&gCtxMutex);
    return TCL_OK;
}

/* m-1: Bare-name registration (e.g., "oralogon" in ::) is preserved for backward
 * compatibility with existing oratcl scripts.  New code should use the
 * namespaced form (::oratcl::oralogon) via [namespace import ::oratcl::*]. */
static void RegisterCommand(Tcl_Interp* ip, Tcl_Namespace* nsPtr, const char* name, Tcl_ObjCmdProc2* proc)
{
    Tcl_CreateObjCommand2(ip, name, proc, NULL, NULL);
    if (nsPtr)
    {
        Tcl_DString ds;
        Tcl_DStringInit(&ds);
        Tcl_DStringAppend(&ds, ORATCL_NAMESPACE, -1);
        Tcl_DStringAppend(&ds, "::", 2);
        Tcl_DStringAppend(&ds, name, -1);
        Tcl_CreateObjCommand2(ip, Tcl_DStringValue(&ds), proc, NULL, NULL);
        Tcl_DStringFree(&ds);
    }
}

static void RegisterCommands(Tcl_Interp* ip)
{
    Tcl_Namespace* nsPtr = Tcl_FindNamespace(ip, ORATCL_NAMESPACE, NULL, 0);
    if (!nsPtr)
        nsPtr = Tcl_CreateNamespace(ip, ORATCL_NAMESPACE, NULL, NULL);

    RegisterCommand(ip, nsPtr, "oralogon", Oradpi_Cmd_Logon);
    RegisterCommand(ip, nsPtr, "oralogoff", Oradpi_Cmd_Logoff);
    RegisterCommand(ip, nsPtr, "oraconfig", Oradpi_Cmd_Config);
    RegisterCommand(ip, nsPtr, "orainfo", Oradpi_Cmd_Info);
    RegisterCommand(ip, nsPtr, "oraopen", Oradpi_Cmd_Open);
    RegisterCommand(ip, nsPtr, "oraclose", Oradpi_Cmd_Close);
    RegisterCommand(ip, nsPtr, "orastmt", Oradpi_Cmd_Stmt);
    RegisterCommand(ip, nsPtr, "oraparse", Oradpi_Cmd_Parse);
    RegisterCommand(ip, nsPtr, "orasql", Oradpi_Cmd_StmtSql);
    RegisterCommand(ip, nsPtr, "orabind", Oradpi_Cmd_Orabind);
    RegisterCommand(ip, nsPtr, "orabindexec", Oradpi_Cmd_Orabindexec);
    RegisterCommand(ip, nsPtr, "oraexec", Oradpi_Cmd_Exec);
    RegisterCommand(ip, nsPtr, "oraplexec", Oradpi_Cmd_Plexec);
    RegisterCommand(ip, nsPtr, "orafetch", Oradpi_Cmd_Fetch);
    RegisterCommand(ip, nsPtr, "oracols", Oradpi_Cmd_Cols);
    RegisterCommand(ip, nsPtr, "oradesc", Oradpi_Cmd_Desc);
    RegisterCommand(ip, nsPtr, "oramsg", Oradpi_Cmd_Msg);
    RegisterCommand(ip, nsPtr, "oralob", Oradpi_Cmd_Lob);
    RegisterCommand(ip, nsPtr, "oraautocommit", Oradpi_Cmd_Autocommit);
    RegisterCommand(ip, nsPtr, "oraautocom", Oradpi_Cmd_Autocommit);
    RegisterCommand(ip, nsPtr, "oracommit", Oradpi_Cmd_Commit);
    RegisterCommand(ip, nsPtr, "oraroll", Oradpi_Cmd_Rollback);
    RegisterCommand(ip, nsPtr, "orarollback", Oradpi_Cmd_Rollback);
    RegisterCommand(ip, nsPtr, "orabreak", Oradpi_Cmd_Break);
    RegisterCommand(ip, nsPtr, "oraexecasync", Oradpi_Cmd_ExecAsync);
    RegisterCommand(ip, nsPtr, "orawaitasync", Oradpi_Cmd_WaitAsync);

    /* Export all commands so users can [namespace import ::oratcl::*] */
    if (nsPtr)
        Tcl_Export(ip, nsPtr, "ora*", 0);
}

#define ORATCL_INTERP_MARK "oradpi.loaded"

static void Oratcl_InterpDeleteProc(void* clientData, Tcl_Interp* ip)
{
    (void)clientData;
    (void)ip;
    /* Intentionally empty: actual cleanup is handled by the AssocData delete
     * procs registered by each subsystem ("oradpi", "oradpi.bindstore",
     * "oradpi.pending").  This proc exists solely as the delete callback for
     * the ORATCL_INTERP_MARK sentinel that prevents double-registration of
     * commands when the package is loaded more than once in the same interp. */
}

/* S-5: Centralized version strings.
 * ORATCL_VERSION: the oratcl package version — must match configure.ac,
 * pkgIndex.tcl, and the Makefile's PACKAGE_VERSION.
 * ORATCL_TCL_MIN: minimum Tcl version required by this extension. */
#define ORATCL_VERSION "9.0"
#define ORATCL_TCL_MIN "9.0"

DLLEXPORT int oratcl_Init(Tcl_Interp* ip)
{
    if (Tcl_InitStubs(ip, ORATCL_TCL_MIN, 0) == NULL)
        return TCL_ERROR;
    if (Oradpi_DpiContextEnsure(ip) != TCL_OK)
        return TCL_ERROR;

    if (Tcl_GetAssocData(ip, ORATCL_INTERP_MARK, NULL) == NULL)
    {
        RegisterCommands(ip);
        Tcl_SetAssocData(ip, ORATCL_INTERP_MARK, Oratcl_InterpDeleteProc, (void*)1);
    }
    return Tcl_PkgProvide(ip, "oratcl", ORATCL_VERSION);
}

DLLEXPORT int oratcl_SafeInit(Tcl_Interp* ip)
{
    if (Tcl_InitStubs(ip, ORATCL_TCL_MIN, 0) == NULL)
        return TCL_ERROR;
    Tcl_SetObjResult(ip, Tcl_NewStringObj("oratcl: cannot load into a safe interpreter (database access not permitted)", -1));
    return TCL_ERROR;
}
