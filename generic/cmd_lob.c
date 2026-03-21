/*
 *  cmd_lob.c --
 *
 *    LOB handle operations (size/read/write/trim/close).
 *
 *        - Thin wrappers over ODPI LOB APIs with Oratcl handle naming semantics.
 *        - Worker-thread friendly: I/O paths hold strong references to underlying ODPI handles.
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

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

int Oradpi_Cmd_Lob(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

static const char* const lobSubcmds[] = {"size", "read", "write", "trim", "close", NULL};
enum LobSubcmdIdx
{
    LOB_SIZE,
    LOB_READ,
    LOB_WRITE,
    LOB_TRIM,
    LOB_CLOSE
};

static const char* const lobReadOpts[] = {"-offset", "-amount", NULL};
enum LobReadOptIdx
{
    LOBR_OFFSET,
    LOBR_AMOUNT
};

/*
 * oralob subcommand lob-handle ?args...?
 *
 *   Subcommands: size, read ?-offset N? ?-amount N?, write data ?-offset N?,
 *   trim newSize, close.  Operates on LOB handles returned by orafetch when
 *   inlineLobs is disabled.
 *   Returns: size (size), byte data (read), 0 (write/trim/close).
 *   Errors:  ODPI-C LOB errors; invalid handle; excessive read size (256 MB cap).
 *   Thread-safety: safe — all ODPI LOB calls are serialized via the shared
 *   per-dpiConn gate (l->shared), ensuring correct behavior under
 *   cross-interp adoption.
 */
int Oradpi_Cmd_Lob(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "subcommand lob-handle ?args...?");
        return TCL_ERROR;
    }

    int subIdx;
    if (Tcl_GetIndexFromObj(ip, objv[1], lobSubcmds, "subcommand", 0, &subIdx) != TCL_OK)
        return TCL_ERROR;

    OradpiLob* l = Oradpi_LookupLob(ip, objv[2]);
    if (!l || !l->lob)
        return Oradpi_SetError(ip, NULL, -1, "invalid lob handle");

    switch ((enum LobSubcmdIdx)subIdx)
    {
        case LOB_SIZE:
        {
            uint64_t sz = 0;
            Oradpi_SharedConnGateEnter(l->shared);
            int ok = dpiLob_getSize(l->lob, &sz);
            Oradpi_SharedConnGateLeave(l->shared);
            if (ok != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_getSize");
            Tcl_SetObjResult(ip, Tcl_NewWideIntObj((Tcl_WideInt)sz));
            return TCL_OK;
        }
        case LOB_READ:
        {
            uint64_t offset = 1, amount = 0, lobSize = 0;
            for (Tcl_Size i = 3; i < objc; i++)
            {
                int optIdx;
                if (Tcl_GetIndexFromObj(ip, objv[i], lobReadOpts, "option", 0, &optIdx) != TCL_OK)
                    return TCL_ERROR;
                if (i + 1 >= objc)
                {
                    Tcl_WrongNumArgs(ip, 1, objv, "read lob-handle ?-offset off? ?-amount amt?");
                    return TCL_ERROR;
                }
                Tcl_WideInt w;
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &w) != TCL_OK)
                    return TCL_ERROR;
                switch ((enum LobReadOptIdx)optIdx)
                {
                    case LOBR_OFFSET:
                        if (w < 1)
                            return Oradpi_SetError(ip, (OradpiBase*)l, -1, "oralob read: -offset must be >= 1");
                        offset = (uint64_t)w;
                        break;
                    case LOBR_AMOUNT:
                        if (w < 0)
                            return Oradpi_SetError(ip, (OradpiBase*)l, -1, "oralob read: -amount must be >= 0");
                        amount = (uint64_t)w;
                        break;
                }
            }
            if (amount == 0)
            {
                Oradpi_SharedConnGateEnter(l->shared);
                int ok = dpiLob_getSize(l->lob, &lobSize);
                Oradpi_SharedConnGateLeave(l->shared);
                if (ok != DPI_SUCCESS)
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_getSize");
                if (lobSize >= offset)
                    amount = lobSize - offset + 1;
            }
            if (amount == 0)
            {
                Tcl_SetObjResult(ip, Tcl_NewObj());
                return TCL_OK;
            }

            uint64_t capBytes = 0;
            Oradpi_SharedConnGateEnter(l->shared);
            int bufOk = dpiLob_getBufferSize(l->lob, amount, &capBytes);
            Oradpi_SharedConnGateLeave(l->shared);
            if (bufOk != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_getBufferSize");

            /* Guard against excessive allocations (default cap: 256 MB) */
            const uint64_t MAX_LOB_READ_BYTES = (256ULL << 20);
            if (capBytes > MAX_LOB_READ_BYTES)
            {
                return Oradpi_SetError(ip,
                                       (OradpiBase*)l,
                                       -1,
                                       "oralob read: requested size exceeds 256 MB safety limit; "
                                       "use -amount with smaller chunks");
            }

            size_t bufBytes = 0;
            if (Oradpi_CheckedU64ToSizeT(ip, capBytes, &bufBytes, "oralob read buffer") != TCL_OK)
                return TCL_ERROR;
            char* buf = (char*)Tcl_Alloc(bufBytes);
            uint64_t gotBytes = capBytes;
            Oradpi_SharedConnGateEnter(l->shared);
            int readOk = dpiLob_readBytes(l->lob, offset, amount, buf, &gotBytes);
            Oradpi_SharedConnGateLeave(l->shared);
            if (readOk != DPI_SUCCESS)
            {
                Tcl_Free(buf);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_readBytes");
            }
            Tcl_SetObjResult(ip, Tcl_NewByteArrayObj((const unsigned char*)buf, (Tcl_Size)gotBytes));
            Tcl_Free(buf);
            return TCL_OK;
        }
        case LOB_WRITE:
        {
            if (objc < 4)
            {
                Tcl_WrongNumArgs(ip, 1, objv, "write lob-handle data ?-offset off?");
                return TCL_ERROR;
            }
            Tcl_Size bl = 0;
            unsigned char* bv = Tcl_GetByteArrayFromObj(objv[3], &bl);
            uint64_t offset = 0; /* 0 = sentinel meaning "append" */
            for (Tcl_Size i = 4; i < objc; i++)
            {
                const char* o = Tcl_GetString(objv[i]);
                if (strcmp(o, "-offset") == 0 && i + 1 < objc)
                {
                    Tcl_WideInt w;
                    if (Tcl_GetWideIntFromObj(ip, objv[++i], &w) != TCL_OK)
                        return TCL_ERROR;
                    if (w < 1)
                        return Oradpi_SetError(ip, (OradpiBase*)l, -1, "oralob write: -offset must be >= 1");
                    offset = (uint64_t)w;
                }
                else
                    return Oradpi_SetError(ip, (OradpiBase*)l, -1, "unknown option, expected -offset");
            }
            if (bl > 0)
            {
                /* When no -offset is given, append after current content */
                if (offset == 0)
                {
                    uint64_t sz = 0;
                    Oradpi_SharedConnGateEnter(l->shared);
                    int sok = dpiLob_getSize(l->lob, &sz);
                    Oradpi_SharedConnGateLeave(l->shared);
                    if (sok != DPI_SUCCESS)
                        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_getSize");
                    offset = sz + 1;
                }
                Oradpi_SharedConnGateEnter(l->shared);
                int wok = dpiLob_writeBytes(l->lob, offset, (const char*)bv, (uint64_t)bl);
                Oradpi_SharedConnGateLeave(l->shared);
                if (wok != DPI_SUCCESS)
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_writeBytes");
            }
            Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
            return TCL_OK;
        }
        case LOB_TRIM:
        {
            if (objc != 4)
            {
                Tcl_WrongNumArgs(ip, 1, objv, "trim lob-handle newSize");
                return TCL_ERROR;
            }
            Tcl_WideInt w;
            if (Tcl_GetWideIntFromObj(ip, objv[3], &w) != TCL_OK)
                return TCL_ERROR;
            if (w < 0)
                return Oradpi_SetError(ip, (OradpiBase*)l, -1, "oralob trim: newSize must be >= 0");
            Oradpi_SharedConnGateEnter(l->shared);
            int tok = dpiLob_trim(l->lob, (uint64_t)w);
            Oradpi_SharedConnGateLeave(l->shared);
            if (tok != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)l, "dpiLob_trim");
            Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
            return TCL_OK;
        }
        case LOB_CLOSE:
        {
            /* Remove from interp hash table first, then delegate to Oradpi_FreeLob
             * which handles gated dpiLob_close, dpiLob_release, name, msg, and Tcl_Free. */
            OradpiInterpState* st = (OradpiInterpState*)Tcl_GetAssocData(ip, "oradpi", NULL);
            if (st && l->base.name)
            {
                const char* lname = Tcl_GetString(l->base.name);
                Tcl_HashEntry* he = Tcl_FindHashEntry(&st->lobs, lname);
                if (he)
                    Tcl_DeleteHashEntry(he);
            }
            Oradpi_FreeLob(l);
            Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
            return TCL_OK;
        }
    }
    /* unreachable */
    return TCL_ERROR;
}
