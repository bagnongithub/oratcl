/*
 *  cmd_stmt.c --
 *
 *    Statement lifecycle and configuration (open/close/parse and stmt‑level options).
 *
 *        - Applies fetch/prefetch and mode settings; maintains per‑interp caches with no cross‑interp sharing.
 *        - Safe in multi‑threaded builds: statement objects guard ODPI handles and are refcounted per interp.
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

#ifndef DPI_DEFAULT_FETCH_ARRAY_SIZE
#define DPI_DEFAULT_FETCH_ARRAY_SIZE 100
#endif

void       Oradpi_BindStoreForget(Tcl_Interp *, const char *);
void       Oradpi_PendingsForget(Tcl_Interp *, const char *);

static int Oradpi_ConfigConn(Tcl_Interp *ip, OradpiConn *co, Tcl_Size objc, Tcl_Obj *const objv[]);
static int Oradpi_ConfigStmt(Tcl_Interp *ip, OradpiStmt *s, Tcl_Size objc, Tcl_Obj *const objv[]);

int        Oradpi_Cmd_Stmt(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    return Oradpi_Cmd_Open(cd, ip, objc, objv);
}

int Oradpi_Cmd_Config(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "handle ?name ?value??");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (s)
        return Oradpi_ConfigStmt(ip, s, objc, objv);
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (co)
        return Oradpi_ConfigConn(ip, co, objc, objv);
    return Oradpi_SetError(ip, NULL, -1, "invalid handle");
}

static int Oradpi_ConfigConn(Tcl_Interp *ip, OradpiConn *co, Tcl_Size objc, Tcl_Obj *const objv[]) {
    if (objc == 2) {
        uint32_t v   = 0;
        Tcl_Obj *res = Tcl_NewListObj(0, NULL);
        if (co->conn && dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
            co->stmtCacheSize = v;
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("stmtcachesize", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->stmtCacheSize));

        uint32_t fas = co->fetchArraySize ? co->fetchArraySize : DPI_DEFAULT_FETCH_ARRAY_SIZE;
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("fetcharraysize", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)fas));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("prefetchrows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->prefetchRows));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("prefetchmemory", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->prefetchMemory));

        if (co->conn && dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
            co->callTimeout = v;
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("calltimeout", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->callTimeout));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("inlineLobs", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewBooleanObj(co->inlineLobs ? 1 : 0));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foMaxAttempts", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->foMaxAttempts));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foBackoffMs", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->foBackoffMs));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foBackoffFactor", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewDoubleObj(co->foBackoffFactor));

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foErrorClasses", -1));
        Tcl_Obj *classes = Tcl_NewListObj(0, NULL);
        if (co->foErrorClasses & 0x01)
            Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("network", -1));
        if (co->foErrorClasses & 0x02)
            Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("connlost", -1));
        Tcl_ListObjAppendElement(ip, res, classes);

        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("foDebounceMs", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)co->foDebounceMs));

        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }

    if (objc == 3) {
        const char *name = Tcl_GetString(objv[2]);
        if (name[0] == '-')
            name++;
        if (strcmp(name, "stmtcachesize") == 0) {
            uint32_t v = co->stmtCacheSize;
            if (co->conn && dpiConn_getStmtCacheSize(co->conn, &v) == DPI_SUCCESS)
                co->stmtCacheSize = v;
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->stmtCacheSize));
            return TCL_OK;
        }
        if (strcmp(name, "fetcharraysize") == 0) {
            uint32_t fas = co->fetchArraySize ? co->fetchArraySize : DPI_DEFAULT_FETCH_ARRAY_SIZE;
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)fas));
            return TCL_OK;
        }
        if (strcmp(name, "prefetchrows") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->prefetchRows));
            return TCL_OK;
        }
        if (strcmp(name, "prefetchmemory") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->prefetchMemory));
            return TCL_OK;
        }
        if (strcmp(name, "calltimeout") == 0) {
            uint32_t v = co->callTimeout;
            if (co->conn && dpiConn_getCallTimeout(co->conn, &v) == DPI_SUCCESS)
                co->callTimeout = v;
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->callTimeout));
            return TCL_OK;
        }
        if (strcmp(name, "inlineLobs") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewBooleanObj(co->inlineLobs ? 1 : 0));
            return TCL_OK;
        }
        if (strcmp(name, "foMaxAttempts") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->foMaxAttempts));
            return TCL_OK;
        }
        if (strcmp(name, "foBackoffMs") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->foBackoffMs));
            return TCL_OK;
        }
        if (strcmp(name, "foBackoffFactor") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewDoubleObj(co->foBackoffFactor));
            return TCL_OK;
        }
        if (strcmp(name, "foErrorClasses") == 0) {
            Tcl_Obj *classes = Tcl_NewListObj(0, NULL);
            if (co->foErrorClasses & 0x01)
                Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("network", -1));
            if (co->foErrorClasses & 0x02)
                Tcl_ListObjAppendElement(ip, classes, Tcl_NewStringObj("connlost", -1));
            Tcl_SetObjResult(ip, classes);
            return TCL_OK;
        }
        if (strcmp(name, "foDebounceMs") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)co->foDebounceMs));
            return TCL_OK;
        }
        return Oradpi_SetError(ip, (OradpiBase *)co, -1, "unknown option");
    }

    if ((objc % 2) != 0) {
        Tcl_WrongNumArgs(ip, 2, objv, "?-name value ...?");
        return TCL_ERROR;
    }
    for (Tcl_Size i = 2; i < objc; i += 2) {
        const char *name = Tcl_GetString(objv[i]);
        if (name[0] == '-')
            name++;
        if (strcmp(name, "stmtcachesize") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            if (v < 0)
                v = 0;
            co->stmtCacheSize = (uint32_t)v;
            if (co->conn && dpiConn_setStmtCacheSize(co->conn, co->stmtCacheSize) != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_setStmtCacheSize");
        } else if (strcmp(name, "fetcharraysize") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->fetchArraySize = (uint32_t)(v > 0 ? v : DPI_DEFAULT_FETCH_ARRAY_SIZE);
        } else if (strcmp(name, "prefetchrows") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->prefetchRows = (uint32_t)(v >= 0 ? v : 0);
        } else if (strcmp(name, "prefetchmemory") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->prefetchMemory = (uint32_t)(v >= 0 ? v : 0);
        } else if (strcmp(name, "calltimeout") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->callTimeout = (uint32_t)(v >= 0 ? v : 0);
            if (co->conn && dpiConn_setCallTimeout(co->conn, co->callTimeout) != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)co, "dpiConn_setCallTimeout");
        } else if (strcmp(name, "inlineLobs") == 0) {
            int v = 0;
            if (Tcl_GetBooleanFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->inlineLobs = v ? 1 : 0;
        } else if (strcmp(name, "foMaxAttempts") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->foMaxAttempts = (uint32_t)(v >= 0 ? v : 0);
        } else if (strcmp(name, "foBackoffMs") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->foBackoffMs = (uint32_t)(v >= 0 ? v : 0);
        } else if (strcmp(name, "foBackoffFactor") == 0) {
            double d = 0.0;
            if (Tcl_GetDoubleFromObj(ip, objv[i + 1], &d) != TCL_OK)
                return TCL_ERROR;
            co->foBackoffFactor = d;
        } else if (strcmp(name, "foErrorClasses") == 0) {
            Tcl_Obj **el = NULL;
            Tcl_Size  n  = 0;
            if (Tcl_ListObjGetElements(ip, objv[i + 1], &n, &el) != TCL_OK)
                return TCL_ERROR;
            uint32_t m = 0;
            for (Tcl_Size k = 0; k < n; k++) {
                const char *t = Tcl_GetString(el[k]);
                if (strcmp(t, "network") == 0)
                    m |= 0x01;
                else if (strcmp(t, "connlost") == 0)
                    m |= 0x02;
            }
            co->foErrorClasses = m;
        } else if (strcmp(name, "foDebounceMs") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[i + 1], &v) != TCL_OK)
                return TCL_ERROR;
            co->foDebounceMs = (uint32_t)(v >= 0 ? v : 0);
        } else
            return Oradpi_SetError(ip, (OradpiBase *)co, -1, "unknown option");
    }
    Tcl_Obj *tmpv[2] = {objv[0], objv[1]};
    return Oradpi_ConfigConn(ip, co, 2, tmpv);
}

static int Oradpi_ConfigStmt(Tcl_Interp *ip, OradpiStmt *s, Tcl_Size objc, Tcl_Obj *const objv[]) {
    if (objc == 2) {
        Tcl_Obj *res = Tcl_NewListObj(0, NULL);
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("fetchrows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)s->fetchArray));
        uint32_t pr = s->owner ? s->owner->prefetchRows : 0;
        if (s->stmt)
            (void)dpiStmt_getPrefetchRows(s->stmt, &pr);
        Tcl_ListObjAppendElement(ip, res, Tcl_NewStringObj("prefetchrows", -1));
        Tcl_ListObjAppendElement(ip, res, Tcl_NewIntObj((int)pr));
        Tcl_SetObjResult(ip, res);
        return TCL_OK;
    }
    if (objc == 3) {
        const char *name = Tcl_GetString(objv[2]);
        if (name[0] == '-')
            name++;
        if (strcmp(name, "fetchrows") == 0) {
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)s->fetchArray));
            return TCL_OK;
        }
        if (strcmp(name, "prefetchrows") == 0) {
            uint32_t pr = s->owner ? s->owner->prefetchRows : 0;
            if (s->stmt)
                (void)dpiStmt_getPrefetchRows(s->stmt, &pr);
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)pr));
            return TCL_OK;
        }
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "unknown option");
    }
    if (objc == 4) {
        const char *name = Tcl_GetString(objv[2]);
        if (name[0] == '-')
            name++;
        if (strcmp(name, "fetchrows") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[3], &v) != TCL_OK)
                return TCL_ERROR;
            s->fetchArray = (uint32_t)(v > 0 ? v : DPI_DEFAULT_FETCH_ARRAY_SIZE);
            if (s->stmt)
                dpiStmt_setFetchArraySize(s->stmt, s->fetchArray);
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)s->fetchArray));
            return TCL_OK;
        }
        if (strcmp(name, "prefetchrows") == 0) {
            int v = 0;
            if (Tcl_GetIntFromObj(ip, objv[3], &v) != TCL_OK)
                return TCL_ERROR;
            uint32_t pr = (uint32_t)(v >= 0 ? v : 0);
            if (s->stmt)
                (void)dpiStmt_setPrefetchRows(s->stmt, pr);
            if (s->owner)
                s->owner->prefetchRows = pr;
            Tcl_SetObjResult(ip, Tcl_NewIntObj((int)pr));
            return TCL_OK;
        }
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "unknown option");
    }
    Tcl_WrongNumArgs(ip, 1, objv, "handle ?name ?value??");
    return TCL_ERROR;
}

int Oradpi_Cmd_Open(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle");
        return TCL_ERROR;
    }
    OradpiConn *co = Oradpi_LookupConn(ip, objv[1]);
    if (!co)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");
    OradpiStmt *s = Oradpi_NewStmt(ip, co);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "cannot allocate statement");
    Tcl_SetObjResult(ip, s->base.name);
    return TCL_OK;
}

int Oradpi_Cmd_Close(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s) {
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    }

    (void)Oradpi_StmtWaitForAsync(s, 1 /* cancel */, 0 /* no timeout */);

    {
        extern void Oradpi_BindStoreForget(Tcl_Interp *, const char *);
        extern void Oradpi_PendingsForget(Tcl_Interp *, const char *);
        const char *stmtKey = Tcl_GetString(s->base.name);
        Oradpi_BindStoreForget(ip, stmtKey);
        Oradpi_PendingsForget(ip, stmtKey);
    }

    if (s->stmt) {
        dpiStmt_close(s->stmt, NULL, 0);
        dpiStmt_release(s->stmt);
        s->stmt = NULL;
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Parse(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle sql-text");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    const char *sql = Tcl_GetString(objv[2]);

    (void)Oradpi_StmtWaitForAsync(s, 1, 0);
    const char *stmtKey = Tcl_GetString(s->base.name);
    Oradpi_BindStoreForget(ip, stmtKey);
    Oradpi_PendingsForget(ip, stmtKey);

    if (s->stmt) {
        dpiStmt_close(s->stmt, NULL, 0);
        dpiStmt_release(s->stmt);
        s->stmt = NULL;
    }

    if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)strlen(sql), NULL, 0, &s->stmt) != DPI_SUCCESS) {
        Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_prepareStmt");
        Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiConn_prepareStmt");
        return TCL_ERROR;
    }

    if (s->fetchArray)
        dpiStmt_setFetchArraySize(s->stmt, s->fetchArray);
    if (s->owner && s->owner->prefetchRows)
        dpiStmt_setPrefetchRows(s->stmt, s->owner->prefetchRows);
    Oradpi_UpdateStmtType(s);

    dpiStmtInfo info;
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
        Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getInfo");
        Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiStmt_getInfo");
        return TCL_ERROR;
    }
    if (info.isQuery) {
        uint32_t bindCount = 0;
        if (dpiStmt_getBindCount(s->stmt, &bindCount) != DPI_SUCCESS) {
            Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getBindCount");
            Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiStmt_getBindCount");
            return TCL_ERROR;
        }
        if (bindCount == 0) {
            uint32_t numQueryCols = 0;
            if (dpiStmt_execute(s->stmt, DPI_MODE_EXEC_DEFAULT, &numQueryCols) != DPI_SUCCESS) {
                Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_execute");
                Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiStmt_execute");
                return TCL_ERROR;
            }
        }
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
