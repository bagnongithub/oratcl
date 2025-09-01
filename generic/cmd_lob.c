/*
 *  cmd_lob.c --
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

#include <string.h>

#include "cmd_int.h"
#include "dpi.h"
#include "state.h"

int Oradpi_Cmd_Lob(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "subcommand lob-handle ?args...?");
        return TCL_ERROR;
    }
    const char *sub = Tcl_GetString(objv[1]);
    OradpiLob  *l   = Oradpi_LookupLob(ip, objv[2]);
    if (!l || !l->lob)
        return Oradpi_SetError(ip, NULL, -1, "invalid lob handle");

    if (strcmp(sub, "size") == 0) {
        uint64_t sz = 0;
        if (dpiLob_getSize(l->lob, &sz) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)l, "dpiLob_getSize");
        Tcl_SetObjResult(ip, Tcl_NewWideIntObj((Tcl_WideInt)sz));
        return TCL_OK;
    } else if (strcmp(sub, "read") == 0) {
        uint64_t offset = 1, amount = 0, lobSize = 0;
        for (Tcl_Size i = 3; i < objc; i++) {
            const char *o = Tcl_GetString(objv[i]);
            if (strcmp(o, "-offset") == 0 && i + 1 < objc) {
                Tcl_WideInt w;
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &w) != TCL_OK)
                    return TCL_ERROR;
                offset = (uint64_t)w;
            } else if (strcmp(o, "-amount") == 0 && i + 1 < objc) {
                Tcl_WideInt w;
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &w) != TCL_OK)
                    return TCL_ERROR;
                amount = (uint64_t)w;
            } else {
                return Oradpi_SetError(ip, (OradpiBase *)l, -1, "unknown option");
            }
        }
        if (amount == 0) {
            if (dpiLob_getSize(l->lob, &lobSize) != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)l, "dpiLob_getSize");
            if (lobSize >= offset)
                amount = lobSize - offset + 1;
        }
        if (amount == 0) {
            Tcl_SetObjResult(ip, Tcl_NewObj());
            return TCL_OK;
        }

        uint64_t capBytes = 0;
        if (dpiLob_getBufferSize(l->lob, amount, &capBytes) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)l, "dpiLob_getBufferSize");

        char    *buf      = (char *)Tcl_Alloc((size_t)capBytes);
        uint64_t gotBytes = capBytes; /* IN: capacity; OUT: bytes actually read */
        if (dpiLob_readBytes(l->lob, offset, amount, buf, &gotBytes) != DPI_SUCCESS) {
            Tcl_Free(buf);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)l, "dpiLob_readBytes");
        }
        Tcl_SetObjResult(ip, Tcl_NewByteArrayObj((const unsigned char *)buf, (Tcl_Size)gotBytes));
        Tcl_Free(buf);
        return TCL_OK;
    } else if (strcmp(sub, "write") == 0) {
        if (objc < 4) {
            Tcl_WrongNumArgs(ip, 1, objv, "write lob-handle data ?-offset off?");
            return TCL_ERROR;
        }
        Tcl_Size       bl     = 0;
        unsigned char *bv     = Tcl_GetByteArrayFromObj(objv[3], &bl);
        uint64_t       offset = 1;
        for (Tcl_Size i = 4; i < objc; i++) {
            const char *o = Tcl_GetString(objv[i]);
            if (strcmp(o, "-offset") == 0 && i + 1 < objc) {
                Tcl_WideInt w;
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &w) != TCL_OK)
                    return TCL_ERROR;
                offset = (uint64_t)w;
            } else
                return Oradpi_SetError(ip, (OradpiBase *)l, -1, "unknown option");
        }
        if (bl > 0 && dpiLob_writeBytes(l->lob, offset, (const char *)bv, (uint64_t)bl) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)l, "dpiLob_writeBytes");
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    } else if (strcmp(sub, "trim") == 0) {
        if (objc != 4) {
            Tcl_WrongNumArgs(ip, 1, objv, "trim lob-handle newSize");
            return TCL_ERROR;
        }
        Tcl_WideInt w;
        if (Tcl_GetWideIntFromObj(ip, objv[3], &w) != TCL_OK)
            return TCL_ERROR;
        if (dpiLob_trim(l->lob, (uint64_t)w) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)l, "dpiLob_trim");
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    } else if (strcmp(sub, "close") == 0) {
        dpiLob_close(l->lob);
        dpiLob_release(l->lob);
        l->lob = NULL;
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    return Oradpi_SetError(ip, (OradpiBase *)l, -1, "unknown lob subcommand");
}
