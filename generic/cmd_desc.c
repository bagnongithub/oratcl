/*
 *  cmd_desc.c --
 *
 *    Metadata and describe utilities for columns and statements.
 *
 *        - Retrieves query/statement descriptors via ODPI-C; presents Oratcl-compatible lists/dicts.
 *        - Read-only operations that are safe across interps and threads.
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

int Oradpi_Cmd_Cols(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
int Oradpi_Cmd_Desc(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/*
 * oracols statement-handle
 *
 *   Returns column metadata for a prepared/executed query as a list of
 *   key-value sublists: {name, nullable, dbSize, charSize} per column.
 *   Returns: list of column info dicts.
 *   Errors:  invalid/unprepared statement; ODPI-C query info errors.
 *   Thread-safety: safe — read-only per-interp state.
 */
int Oradpi_Cmd_Cols(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s || !s->stmt)
        return Oradpi_SetError(ip, NULL, -1, "invalid or unprepared statement");

    uint32_t ncols = 0;
    if (dpiStmt_getNumQueryColumns(s->stmt, &ncols) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getNumQueryColumns");

    Tcl_Obj* res = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(res);
    for (uint32_t i = 1; i <= ncols; i++)
    {
        dpiQueryInfo qi;
        if (dpiStmt_getQueryInfo(s->stmt, i, &qi) != DPI_SUCCESS)
        {
            Tcl_DecrRefCount(res);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getQueryInfo");
        }

        Tcl_Obj* entry = Tcl_NewListObj(0, NULL);
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("name", -1));
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj(qi.name, (Tcl_Size)qi.nameLength));
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("nullable", -1));
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewBooleanObj(qi.nullOk));
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("dbSize", -1));
        Tcl_ListObjAppendElement(ip, entry, Oradpi_NewUInt32Obj(qi.typeInfo.dbSizeInBytes));
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("charSize", -1));
        Tcl_ListObjAppendElement(ip, entry, Oradpi_NewUInt32Obj(qi.typeInfo.sizeInChars));

        Tcl_ListObjAppendElement(ip, res, entry);
    }
    Tcl_SetObjResult(ip, res);
    Tcl_DecrRefCount(res);
    return TCL_OK;
}

/*
 * oradesc logon-handle object-name
 *
 *   Describes a table or view by issuing "SELECT * FROM <object> WHERE 0=1"
 *   and returning column metadata. Object name is validated against a strict
 *   character allowlist (alphanumeric, _, ., $, #) to prevent SQL injection.
 *   Returns: list of {name type} pairs.
 *   Errors:  invalid handle; invalid identifier characters; ODPI-C errors.
 *   Thread-safety: safe — uses a temporary dpiStmt, no shared state.
 */
int Oradpi_Cmd_Desc(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc != 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "logon-handle object-name");
        return TCL_ERROR;
    }
    OradpiConn* co = Oradpi_LookupConn(ip, objv[1]);
    if (!co || !co->conn)
        return Oradpi_SetError(ip, NULL, -1, "invalid logon handle");

    Tcl_Size tl = 0;
    const char* tab = Tcl_GetStringFromObj(objv[2], &tl);

    /* m-7: Validate identifier: only allow alphanumeric, underscore, dot, $, #.
     * Double-quotes are rejected to prevent SQL injection via quoted identifiers.
     * Additional structural checks prevent pathological patterns:
     *   - Empty identifier
     *   - Leading/trailing dots (e.g., ".table", "schema.")
     *   - Consecutive dots (e.g., "schema..table")
     *   - More than one dot (only schema.object is valid) */
    if (tl == 0)
        return Oradpi_SetError(ip, (OradpiBase*)co, -1, "object name is empty");
    if (tab[0] == '.' || tab[tl - 1] == '.')
        return Oradpi_SetError(ip, (OradpiBase*)co, -1, "object name must not start or end with '.'");

    int dotCount = 0;
    for (Tcl_Size ci = 0; ci < tl; ci++)
    {
        char ch = tab[ci];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' ||
              ch == '$' || ch == '#'))
            return Oradpi_SetError(ip, (OradpiBase*)co, -1, "object name contains invalid characters");
        if (ch == '.')
        {
            dotCount++;
            if (dotCount > 1)
                return Oradpi_SetError(
                    ip, (OradpiBase*)co, -1, "object name contains too many '.' separators (max: schema.object)");
            if (ci + 1 < tl && tab[ci + 1] == '.')
                return Oradpi_SetError(ip, (OradpiBase*)co, -1, "object name contains consecutive dots");
        }
    }

    /* Build SQL using Tcl_DString to avoid fixed-size buffer truncation */
    Tcl_DString ds;
    Tcl_DStringInit(&ds);
    Tcl_DStringAppend(&ds, "select * from ", -1);
    Tcl_DStringAppend(&ds, tab, (Tcl_Size)tl);
    Tcl_DStringAppend(&ds, " where 0=1", -1);
    const char* sql = Tcl_DStringValue(&ds);
    Tcl_Size sqlLen = Tcl_DStringLength(&ds);
    if (sqlLen < 0 || (uint64_t)sqlLen > UINT32_MAX)
    {
        Tcl_DStringFree(&ds);
        return Oradpi_SetError(ip, (OradpiBase*)co, -1, "generated SQL too long");
    }

    dpiStmt* stmt = NULL;
    if (dpiConn_prepareStmt(co->conn, 0, sql, (uint32_t)sqlLen, NULL, 0, &stmt) != DPI_SUCCESS)
    {
        Tcl_DStringFree(&ds);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiConn_prepareStmt");
    }
    Tcl_DStringFree(&ds);
    uint32_t cols = 0;
    if (dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, &cols) != DPI_SUCCESS)
    {
        dpiStmt_close(stmt, NULL, 0);
        dpiStmt_release(stmt);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiStmt_execute");
    }

    Tcl_Obj* res = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(res);
    for (uint32_t i = 1; i <= cols; i++)
    {
        dpiQueryInfo qi;
        if (dpiStmt_getQueryInfo(stmt, i, &qi) != DPI_SUCCESS)
        {
            dpiStmt_close(stmt, NULL, 0);
            dpiStmt_release(stmt);
            Tcl_DecrRefCount(res);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiStmt_getQueryInfo");
        }
        Tcl_Obj* entry = Tcl_NewListObj(0, NULL);
        Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj(qi.name, (Tcl_Size)qi.nameLength));
        Tcl_ListObjAppendElement(
            ip, entry, Tcl_NewStringObj(qi.typeInfo.oracleTypeNum == DPI_ORACLE_TYPE_NUMBER ? "NUMBER" : "OTHER", -1));
        Tcl_ListObjAppendElement(ip, res, entry);
    }
    dpiStmt_close(stmt, NULL, 0);
    dpiStmt_release(stmt);
    Tcl_SetObjResult(ip, res);
    Tcl_DecrRefCount(res);
    return TCL_OK;
}
