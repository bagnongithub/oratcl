/*
 *  cmd_logon.c --
 *
 *    Connection management commands and helpers (logon/logoff, pooled and dedicated connections).
 *
 *        - Parses connect strings and external auth; opens dpiConn via ODPI‑C only.
 *        - Registers connections in the current interpreter while avoiding duplicate client loads across interps.
 *        - Thread‑aware: minimal shared mutable state; process‑wide pieces guarded by Tcl mutexes.
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
#include "state.h"

extern dpiContext *Oradpi_GlobalDpiContext;

static void        Oradpi_ParseConnect(const char *cs, const char **user, uint32_t *ulen, const char **pw, uint32_t *plen, const char **db, uint32_t *dblen, int *extAuth) {
    *user = *pw = *db = NULL;
    *ulen = *plen = *dblen = 0;
    *extAuth               = 0;
    if (!cs)
        return;
    const char *at    = strchr(cs, '@');
    const char *slash = strchr(cs, '/');
    if (cs[0] == '/' && (!slash || slash == cs)) {
        *extAuth = 1;
        if (at && at[1]) {
            *db    = at + 1;
            *dblen = (uint32_t)strlen(*db);
        }
        return;
    }
    if (at) {
        if (slash && slash < at) {
            *user = cs;
            *ulen = (uint32_t)(slash - cs);
            *pw   = slash + 1;
            *plen = (uint32_t)(at - slash - 1);
        } else {
            *user = cs;
            *ulen = (uint32_t)(at - cs);
        }
        *db    = at + 1;
        *dblen = (uint32_t)strlen(*db);
    } else {
        if (slash) {
            *user = cs;
            *ulen = (uint32_t)(slash - cs);
            *pw   = slash + 1;
            *plen = (uint32_t)strlen(*pw);
        } else {
            *user = cs;
            *ulen = (uint32_t)strlen(cs);
        }
    }
}

int Oradpi_Cmd_Logon(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv,
                         "connect-str ?-pool min max incr? ?-homogeneous bool? "
                         "?-getmode wait|nowait|forceget|timedwait? "
                         "?-failovercallback proc?");
        return TCL_ERROR;
    }
    const char *connstr = Tcl_GetString(objv[1]);
    int         usePool = 0, minS = 1, maxS = 4, incS = 1, homogeneous = 1;
    int         getmode    = DPI_MODE_POOL_GET_WAIT;
    Tcl_Obj    *failoverCb = NULL;

    for (int i = 2; i < objc; i++) {
        const char *o = Tcl_GetString(objv[i]);
        if (strcmp(o, "-pool") == 0) {
            if (i + 1 >= objc)
                return Oradpi_SetError(ip, NULL, -1, "unknown or incomplete option");
            Tcl_Size  n     = 0;
            Tcl_Obj **elems = NULL;
            if (Tcl_ListObjGetElements(ip, objv[i + 1], &n, &elems) == TCL_OK && n == 3) {
                i++;
                if (Tcl_GetIntFromObj(ip, elems[0], &minS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetIntFromObj(ip, elems[1], &maxS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetIntFromObj(ip, elems[2], &incS) != TCL_OK)
                    return TCL_ERROR;
            } else if (i + 3 < objc) {
                if (Tcl_GetIntFromObj(ip, objv[++i], &minS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetIntFromObj(ip, objv[++i], &maxS) != TCL_OK)
                    return TCL_ERROR;
                if (Tcl_GetIntFromObj(ip, objv[++i], &incS) != TCL_OK)
                    return TCL_ERROR;
            } else
                return Oradpi_SetError(ip, NULL, -1, "unknown or incomplete option");
            usePool = 1;
        } else if (strcmp(o, "-homogeneous") == 0 && i + 1 < objc) {
            if (Tcl_GetBooleanFromObj(ip, objv[++i], &homogeneous) != TCL_OK)
                return TCL_ERROR;
        } else if (strcmp(o, "-getmode") == 0 && i + 1 < objc) {
            const char *m = Tcl_GetString(objv[++i]);
            if (strcmp(m, "wait") == 0)
                getmode = DPI_MODE_POOL_GET_WAIT;
            else if (strcmp(m, "nowait") == 0)
                getmode = DPI_MODE_POOL_GET_NOWAIT;
            else if (strcmp(m, "forceget") == 0)
                getmode = DPI_MODE_POOL_GET_FORCEGET;
            else if (strcmp(m, "timedwait") == 0)
                getmode = DPI_MODE_POOL_GET_TIMEDWAIT;
            else
                return Oradpi_SetError(ip, NULL, -1, "unknown getmode");
        } else if (strcmp(o, "-failovercallback") == 0 && i + 1 < objc) {
            failoverCb = objv[++i];
        } else {
            return Oradpi_SetError(ip, NULL, -1, "unknown or incomplete option");
        }
    }

    const char *user = NULL, *pw = NULL, *db = NULL;
    uint32_t    ulen = 0, plen = 0, dblen = 0;
    int         ext = 0;
    Oradpi_ParseConnect(connstr, &user, &ulen, &pw, &plen, &db, &dblen, &ext);

    dpiCommonCreateParams cparams;
    dpiConnCreateParams   ccp;
    dpiContext_initCommonCreateParams(Oradpi_GlobalDpiContext, &cparams);
    dpiContext_initConnCreateParams(Oradpi_GlobalDpiContext, &ccp);
    ccp.externalAuth = ext;

    dpiConn *conn    = NULL;
    dpiPool *pool    = NULL;
    if (usePool) {
        dpiPoolCreateParams pp;
        dpiContext_initPoolCreateParams(Oradpi_GlobalDpiContext, &pp);
        pp.minSessions      = (uint32_t)minS;
        pp.maxSessions      = (uint32_t)maxS;
        pp.sessionIncrement = (uint32_t)incS;
        pp.homogeneous      = homogeneous;
        pp.externalAuth     = ext;
        if (dpiPool_create(Oradpi_GlobalDpiContext, user, ulen, pw, plen, db, dblen, &cparams, &pp, &pool) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_create");
        dpiPool_setGetMode(pool, (dpiPoolGetMode)getmode);
        if (dpiPool_acquireConnection(pool, NULL, 0, NULL, 0, &ccp, &conn) != DPI_SUCCESS) {
            dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
            dpiPool_release(pool);
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiPool_acquireConnection");
        }
    } else {
        if (dpiConn_create(Oradpi_GlobalDpiContext, user, ulen, pw, plen, db, dblen, &cparams, &ccp, &conn) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, NULL, "dpiConn_create");
    }

    OradpiConn *co = Oradpi_NewConn(ip, conn, pool);
    if (!co) {
        if (conn) {
            dpiConn_close(conn, DPI_MODE_CONN_CLOSE_DEFAULT, NULL, 0);
            dpiConn_release(conn);
        }
        if (pool) {
            dpiPool_close(pool, DPI_MODE_POOL_CLOSE_DEFAULT);
            dpiPool_release(pool);
        }
        return Oradpi_SetError(ip, NULL, -1, "failed to allocate logon handle");
    }

    co->ownerIp          = ip;
    co->ownerTid         = Tcl_GetCurrentThread();
    co->failoverCallback = NULL;
    co->foDebounceMs     = 250;
    co->foTimer          = NULL;
    co->foTimerScheduled = 0;
    co->foPendingMsg     = NULL;
    if (failoverCb) {
        co->failoverCallback = failoverCb;
        Tcl_IncrRefCount(co->failoverCallback);
    }

    Tcl_SetObjResult(ip, co->base.name);
    return TCL_OK;
}

int Oradpi_Cmd_Logoff(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }

    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co) {
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    }

    OradpiInterpState *st = (OradpiInterpState *)Tcl_GetAssocData(ip, "oradpi", NULL);
    if (st && co->base.name) {
        const char    *hname = Tcl_GetString(co->base.name);
        Tcl_HashEntry *e     = Tcl_FindHashEntry(&st->conns, hname);
        if (e)
            Tcl_DeleteHashEntry(e);
    }

    extern void Oradpi_FreeConn(OradpiConn *);
    Oradpi_FreeConn(co);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Autocommit(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle boolean");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    int flag = 0;
    if (Tcl_GetBooleanFromObj(ip, objv[2], &flag) != TCL_OK)
        return TCL_ERROR;
    co->autocommit = flag;
    Tcl_SetObjResult(ip, Tcl_NewIntObj(flag));
    return TCL_OK;
}

int Oradpi_Cmd_Break(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    if (dpiConn_breakExecution(co->conn) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_breakExecution");
    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Info(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    Tcl_Obj *d = Tcl_NewListObj(0, NULL);
    Tcl_ListObjAppendElement(ip, d, Tcl_NewStringObj("autocommit", -1));
    Tcl_ListObjAppendElement(ip, d, Tcl_NewBooleanObj(co->autocommit));
    Tcl_SetObjResult(ip, d);
    return TCL_OK;
}
