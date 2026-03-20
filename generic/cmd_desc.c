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

typedef struct OradpiDescCol
{
    char* name;
    uint32_t nameLen;
    int nullOk;
    uint32_t dbSize;
    uint32_t charSize;
    dpiOracleTypeNum oracleTypeNum;
} OradpiDescCol;

static void FreeDescCols(OradpiDescCol* cols, Tcl_Size n)
{
    if (!cols)
        return;
    for (Tcl_Size i = 0; i < n; i++)
    {
        if (cols[i].name)
            Tcl_Free(cols[i].name);
    }
    Tcl_Free((char*)cols);
}

static int SnapshotStmtCols(Tcl_Interp* ip, OradpiStmt* s, uint32_t ncols, OradpiDescCol* cols)
{
    CONN_GATE_ENTER(s->owner);
    for (uint32_t i = 1; i <= ncols; i++)
    {
        dpiQueryInfo qi;
        size_t nameBytes = 0;
        if (dpiStmt_getQueryInfo(s->stmt, i, &qi) != DPI_SUCCESS)
        {
            CONN_GATE_LEAVE(s->owner);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getQueryInfo");
        }
        if (Oradpi_CheckedAllocBytes(NULL, (Tcl_Size)qi.nameLength + 1, sizeof(char), &nameBytes, "column name copy") != TCL_OK)
        {
            CONN_GATE_LEAVE(s->owner);
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("column name copy is too large"));
            return TCL_ERROR;
        }
        cols[i - 1].name = (char*)Tcl_Alloc(nameBytes);
        memcpy(cols[i - 1].name, qi.name, (size_t)qi.nameLength);
        cols[i - 1].name[qi.nameLength] = '\0';
        cols[i - 1].nameLen = qi.nameLength;
        cols[i - 1].nullOk = qi.nullOk ? 1 : 0;
        cols[i - 1].dbSize = qi.typeInfo.dbSizeInBytes;
        cols[i - 1].charSize = qi.typeInfo.sizeInChars;
        cols[i - 1].oracleTypeNum = qi.typeInfo.oracleTypeNum;
    }
    CONN_GATE_LEAVE(s->owner);
    return TCL_OK;
}

static int SnapshotDescribeCols(Tcl_Interp* ip, OradpiConn* co, dpiStmt* stmt, uint32_t ncols, OradpiDescCol* cols)
{
    CONN_GATE_ENTER(co);
    for (uint32_t i = 1; i <= ncols; i++)
    {
        dpiQueryInfo qi;
        size_t nameBytes = 0;
        if (dpiStmt_getQueryInfo(stmt, i, &qi) != DPI_SUCCESS)
        {
            CONN_GATE_LEAVE(co);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiStmt_getQueryInfo");
        }
        if (Oradpi_CheckedAllocBytes(NULL, (Tcl_Size)qi.nameLength + 1, sizeof(char), &nameBytes, "describe column name copy") !=
            TCL_OK)
        {
            CONN_GATE_LEAVE(co);
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("describe column name copy is too large"));
            return TCL_ERROR;
        }
        cols[i - 1].name = (char*)Tcl_Alloc(nameBytes);
        memcpy(cols[i - 1].name, qi.name, (size_t)qi.nameLength);
        cols[i - 1].name[qi.nameLength] = '\0';
        cols[i - 1].nameLen = qi.nameLength;
        cols[i - 1].oracleTypeNum = qi.typeInfo.oracleTypeNum;
    }
    CONN_GATE_LEAVE(co);
    return TCL_OK;
}

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
    uint32_t ncols = 0;
    size_t colsBytes = 0;
    OradpiDescCol* cols = NULL;
    Tcl_Obj* res = NULL;
    int code = TCL_OK;

    (void)cd;
    if (objc != 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s || !s->stmt)
        return Oradpi_SetError(ip, NULL, -1, "invalid or unprepared statement");

    CONN_GATE_ENTER(s->owner);
    if (dpiStmt_getNumQueryColumns(s->stmt, &ncols) != DPI_SUCCESS)
    {
        CONN_GATE_LEAVE(s->owner);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getNumQueryColumns");
    }
    CONN_GATE_LEAVE(s->owner);

    if (ncols == 0)
    {
        Tcl_SetObjResult(ip, Tcl_NewListObj(0, NULL));
        return TCL_OK;
    }
    if (Oradpi_CheckedAllocBytes(ip, (Tcl_Size)ncols, sizeof(*cols), &colsBytes, "column metadata snapshot") != TCL_OK)
        return TCL_ERROR;
    cols = (OradpiDescCol*)Tcl_Alloc(colsBytes);
    memset(cols, 0, colsBytes);
    if (SnapshotStmtCols(ip, s, ncols, cols) != TCL_OK)
    {
        code = TCL_ERROR;
        goto cleanup;
    }

    res = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(res);
    for (uint32_t i = 0; i < ncols; i++)
    {
        Tcl_Obj* entry = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(entry);
        if (Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("name", -1)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj(cols[i].name, (Tcl_Size)cols[i].nameLen)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("nullable", -1)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Tcl_NewBooleanObj(cols[i].nullOk)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("dbSize", -1)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Oradpi_NewUInt32Obj(cols[i].dbSize)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj("charSize", -1)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, entry, Oradpi_NewUInt32Obj(cols[i].charSize)) != TCL_OK ||
            Tcl_ListObjAppendElement(ip, res, entry) != TCL_OK)
        {
            Tcl_DecrRefCount(entry);
            code = TCL_ERROR;
            goto cleanup;
        }
        Tcl_DecrRefCount(entry);
    }
    Tcl_SetObjResult(ip, res);

cleanup:
    if (res)
        Tcl_DecrRefCount(res);
    FreeDescCols(cols, (Tcl_Size)ncols);
    return code;
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
    dpiStmt* stmt = NULL;
    uint32_t ncols = 0;
    size_t colsBytes = 0;
    OradpiDescCol* cols = NULL;
    Tcl_Obj* res = NULL;
    int code = TCL_OK;

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

    Tcl_DString ds;
    Tcl_DStringInit(&ds);
    Tcl_DStringAppend(&ds, "select * from ", -1);
    Tcl_DStringAppend(&ds, tab, tl);
    Tcl_DStringAppend(&ds, " where 0=1", -1);
    const char* sql = Tcl_DStringValue(&ds);
    Tcl_Size sqlLen = Tcl_DStringLength(&ds);
    if (sqlLen < 0 || (uint64_t)sqlLen > UINT32_MAX)
    {
        Tcl_DStringFree(&ds);
        return Oradpi_SetError(ip, (OradpiBase*)co, -1, "generated SQL too long");
    }

    CONN_GATE_ENTER(co);
    if (dpiConn_prepareStmt(co->conn, 0, sql, (uint32_t)sqlLen, NULL, 0, &stmt) != DPI_SUCCESS)
    {
        CONN_GATE_LEAVE(co);
        Tcl_DStringFree(&ds);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiConn_prepareStmt");
    }
    Tcl_DStringFree(&ds);
    if (dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, &ncols) != DPI_SUCCESS)
    {
        CONN_GATE_LEAVE(co);
        dpiStmt_close(stmt, NULL, 0);
        dpiStmt_release(stmt);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)co, "dpiStmt_execute");
    }
    CONN_GATE_LEAVE(co);

    if (ncols > 0)
    {
        if (Oradpi_CheckedAllocBytes(ip, (Tcl_Size)ncols, sizeof(*cols), &colsBytes, "describe metadata snapshot") != TCL_OK)
        {
            code = TCL_ERROR;
            goto cleanup;
        }
        cols = (OradpiDescCol*)Tcl_Alloc(colsBytes);
        memset(cols, 0, colsBytes);
        if (SnapshotDescribeCols(ip, co, stmt, ncols, cols) != TCL_OK)
        {
            code = TCL_ERROR;
            goto cleanup;
        }
    }

    res = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(res);
    for (uint32_t i = 0; i < ncols; i++)
    {
        Tcl_Obj* entry = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(entry);
        if (Tcl_ListObjAppendElement(ip, entry, Tcl_NewStringObj(cols[i].name, (Tcl_Size)cols[i].nameLen)) != TCL_OK ||
            Tcl_ListObjAppendElement(
                ip, entry, Tcl_NewStringObj(cols[i].oracleTypeNum == DPI_ORACLE_TYPE_NUMBER ? "NUMBER" : "OTHER", -1)) !=
                TCL_OK ||
            Tcl_ListObjAppendElement(ip, res, entry) != TCL_OK)
        {
            Tcl_DecrRefCount(entry);
            code = TCL_ERROR;
            goto cleanup;
        }
        Tcl_DecrRefCount(entry);
    }
    Tcl_SetObjResult(ip, res);

cleanup:
    if (stmt)
    {
        CONN_GATE_ENTER(co);
        dpiStmt_close(stmt, NULL, 0);
        dpiStmt_release(stmt);
        CONN_GATE_LEAVE(co);
    }
    if (res)
        Tcl_DecrRefCount(res);
    FreeDescCols(cols, (Tcl_Size)ncols);
    return code;
}
