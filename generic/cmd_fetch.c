/*
 *  cmd_fetch.c --
 *
 *    Row fetch and result materialization.
 *
 *        - Maps ODPI column types to Tcl objects; supports `-max`, name/position addressing, LOB streaming,
 *          and optional per-row callbacks.
 *        - Per-interp data paths only; no process-global state; thread-safe access to result buffers.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <string.h>

#include "cmd_int.h"
#include "dpi.h"

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static int is_char_type(dpiOracleTypeNum otn);
int Oradpi_Cmd_Fetch(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[]);
static Tcl_Obj* upper_copy(const char* s, uint32_t n);
static Tcl_Obj* ValueToObj(Tcl_Interp* ip, OradpiStmt* st, dpiNativeTypeNum nt, dpiData* d, int colIsChar);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

static int is_char_type(dpiOracleTypeNum otn)
{
    switch (otn)
    {
        case DPI_ORACLE_TYPE_VARCHAR:
        case DPI_ORACLE_TYPE_NVARCHAR:
        case DPI_ORACLE_TYPE_CHAR:
        case DPI_ORACLE_TYPE_NCHAR:
        case DPI_ORACLE_TYPE_CLOB:
        case DPI_ORACLE_TYPE_NCLOB:
        case DPI_ORACLE_TYPE_LONG_VARCHAR:
            return 1;
        default:
            return 0;
    }
}

/* m-3: Use Tcl_UtfToUpper for Unicode-aware uppercasing of column names.
 * Oracle column names may contain accented characters in NLS configurations. */
static Tcl_Obj* upper_copy(const char* s, uint32_t n)
{
    Tcl_DString ds;
    Tcl_DStringInit(&ds);
    Tcl_DStringAppend(&ds, s, (Tcl_Size)n);
    Tcl_UtfToUpper(Tcl_DStringValue(&ds));
    Tcl_Obj* o = Tcl_NewStringObj(Tcl_DStringValue(&ds), Tcl_DStringLength(&ds));
    Tcl_DStringFree(&ds);
    return o;
}

static Tcl_Obj* ValueToObj(Tcl_Interp* ip, OradpiStmt* st, dpiNativeTypeNum nt, dpiData* d, int colIsChar)
{
    if (!d || d->isNull)
        return Tcl_NewObj();

    switch (nt)
    {
        case DPI_NATIVE_TYPE_INT64:
        {
            long long v = d->value.asInt64;
            if (v >= INT_MIN && v <= INT_MAX)
                return Tcl_NewIntObj((int)v);
            return Tcl_NewWideIntObj((Tcl_WideInt)v);
        }
        case DPI_NATIVE_TYPE_UINT64:
        {
            uint64_t uv = d->value.asUint64;
            if (uv <= (uint64_t)INT64_MAX)
                return Tcl_NewWideIntObj((Tcl_WideInt)uv);
            /* Value exceeds signed 64-bit range; return as decimal string */
            return Tcl_ObjPrintf("%" PRIu64, uv);
        }
        case DPI_NATIVE_TYPE_FLOAT:
        {
            double dv = (double)d->value.asFloat;
            if (isfinite(dv))
            {
                double intpart;
                if (modf(dv, &intpart) == 0.0)
                {
                    if (intpart >= (double)INT_MIN && intpart <= (double)INT_MAX)
                        return Tcl_NewIntObj((int)intpart);
                    if (intpart >= (double)LLONG_MIN && intpart <= (double)LLONG_MAX)
                        return Tcl_NewWideIntObj((Tcl_WideInt)((long long)intpart));
                }
            }
            return Tcl_NewDoubleObj(dv);
        }
        case DPI_NATIVE_TYPE_DOUBLE:
        {
            double dv = (double)d->value.asDouble;
            if (isfinite(dv))
            {
                double intpart;
                if (modf(dv, &intpart) == 0.0)
                {
                    if (intpart >= (double)INT_MIN && intpart <= (double)INT_MAX)
                        return Tcl_NewIntObj((int)intpart);
                    if (intpart >= (double)LLONG_MIN && intpart <= (double)LLONG_MAX)
                        return Tcl_NewWideIntObj((Tcl_WideInt)((long long)intpart));
                }
            }
            return Tcl_NewDoubleObj(dv);
        }
        case DPI_NATIVE_TYPE_BOOLEAN:
            return Tcl_NewBooleanObj(d->value.asBoolean ? 1 : 0);
        case DPI_NATIVE_TYPE_TIMESTAMP:
        {
            const dpiTimestamp* ts = &d->value.asTimestamp;
            return Tcl_ObjPrintf("%04d-%02u-%02uT%02u:%02u:%02u.%06u",
                                 ts->year,
                                 ts->month,
                                 ts->day,
                                 ts->hour,
                                 ts->minute,
                                 ts->second,
                                 ts->fsecond / 1000);
        }
        case DPI_NATIVE_TYPE_BYTES:
        {
            const dpiBytes* b = &d->value.asBytes;
            if (!b->ptr)
                return Tcl_NewObj();
            if (colIsChar)
            {
                return Tcl_NewStringObj(b->ptr, (Tcl_Size)b->length);
            }
            else
            {
                return Tcl_NewByteArrayObj((const unsigned char*)b->ptr, (Tcl_Size)b->length);
            }
        }
        case DPI_NATIVE_TYPE_LOB:
        {
            OradpiConn* co = st->owner;
            dpiLob* lob = d->value.asLOB;
            if (!lob)
                return Tcl_NewObj();

            if (co && co->inlineLobs == 0)
            {
                dpiLob_addRef(lob);
                OradpiLob* L = Oradpi_NewLob(ip, lob);
                return L->base.name;
            }

            uint64_t sizeCharsOrBytes = 0;
            if (dpiLob_getSize(lob, &sizeCharsOrBytes) != DPI_SUCCESS)
            {
                Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_getSize");
                return NULL;
            }
            if (sizeCharsOrBytes == 0)
                return Tcl_NewObj();

            uint64_t capBytes = 0;
            if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS)
            {
                Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_getBufferSize");
                return NULL;
            }

            /* M-5 fix: reject inline LOBs exceeding the safety cap instead of
             * silently truncating.  Users should disable inlineLobs and use
             * oralob read -amount to fetch large LOBs in chunks. */
            const uint64_t MAX_INLINE = (1u << 20);
            if (capBytes > MAX_INLINE && capBytes > 0)
            {
                Oradpi_SetError(ip,
                                (OradpiBase*)st,
                                -1,
                                "LOB value exceeds 1 MB inline limit; "
                                "disable inlineLobs and use oralob read -amount for large values");
                return NULL;
            }

            size_t bufBytes = 0;
            if (Oradpi_CheckedU64ToSizeT(ip, capBytes, &bufBytes, "inline LOB buffer") != TCL_OK)
                return NULL;
            char* buf = (char*)Tcl_Alloc(bufBytes);
            uint64_t gotBytes = capBytes;
            if (dpiLob_readBytes(lob, 1, sizeCharsOrBytes, buf, &gotBytes) != DPI_SUCCESS)
            {
                Tcl_Free(buf);
                Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_readBytes");
                return NULL;
            }

            Tcl_Obj* out = colIsChar ? Tcl_NewStringObj(buf, (Tcl_Size)gotBytes)
                                     : Tcl_NewByteArrayObj((unsigned char*)buf, (Tcl_Size)gotBytes);
            Tcl_Free(buf);
            return out;
        }
        default:
            return Tcl_NewObj();
    }
}

/*
 * orafetch statement-handle ?-datavariable varName? ?-dataarray arrName?
 *         ?-indexbyname? ?-indexbynumber? ?-command script? ?-max N?
 *         ?-resultvariable varName? ?-returnrows? ?-asdict?
 *
 *   Fetches rows from a previously executed query. By default returns all
 *   rows as a Tcl list of row-lists. Options control variable binding,
 *   per-row callbacks, and result format (dict, array, etc.).
 *   Returns: row list (default), 0 (rows fetched), or 1403 (no data found).
 *   Errors:  ODPI-C fetch errors; invalid handle; async busy.
 *   Thread-safety: safe — per-interp state only; blocks if async busy.
 */
int Oradpi_Cmd_Fetch(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
        return TCL_ERROR;
    }
    OradpiStmt* st = Oradpi_LookupStmt(ip, objv[1]);
    if (!st)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!st->stmt)
        return Oradpi_SetError(ip, (OradpiBase*)st, -1, "statement is not prepared or connection closed");
    if (Oradpi_StmtIsAsyncBusy(st))
        return Oradpi_SetError(ip, (OradpiBase*)st, -1, "statement is busy (async operation in progress)");

    Tcl_Obj* dataVar = NULL;
    Tcl_Obj* dataArray = NULL;
    int indexByName = 0, indexByNumber = 0;
    Tcl_Obj* cmd = NULL;
    Tcl_WideInt maxRows = 0;
    Tcl_Obj* resultVar = NULL;
    int returnRows = 1;
    int asDict = 0;

    static const char* const fetchOpts[] = {"-datavariable",
                                            "-dataarray",
                                            "-indexbyname",
                                            "-indexbynumber",
                                            "-command",
                                            "-max",
                                            "-resultvariable",
                                            "-returnrows",
                                            "-asdict",
                                            NULL};
    enum FetchOptIdx
    {
        FOPT_DATAVAR,
        FOPT_DATAARRAY,
        FOPT_BYNAME,
        FOPT_BYNUMBER,
        FOPT_COMMAND,
        FOPT_MAX,
        FOPT_RESULTVAR,
        FOPT_RETURNROWS,
        FOPT_ASDICT
    };

    for (Tcl_Size i = 2; i < objc; i++)
    {
        int optIdx;
        if (Tcl_GetIndexFromObj(ip, objv[i], fetchOpts, "option", 0, &optIdx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum FetchOptIdx)optIdx)
        {
            case FOPT_DATAVAR:
                if (i + 1 >= objc)
                {
                    Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                    return TCL_ERROR;
                }
                dataVar = objv[++i];
                returnRows = 0;
                break;
            case FOPT_DATAARRAY:
                if (i + 1 >= objc)
                {
                    Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                    return TCL_ERROR;
                }
                dataArray = objv[++i];
                returnRows = 0;
                break;
            case FOPT_BYNAME:
                indexByName = 1;
                returnRows = 0;
                break;
            case FOPT_BYNUMBER:
                indexByNumber = 1;
                returnRows = 0;
                break;
            case FOPT_COMMAND:
                if (i + 1 >= objc)
                {
                    Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                    return TCL_ERROR;
                }
                cmd = objv[++i];
                returnRows = 0;
                break;
            case FOPT_MAX:
                if (i + 1 >= objc)
                {
                    Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                    return TCL_ERROR;
                }
                if (Tcl_GetWideIntFromObj(ip, objv[++i], &maxRows) != TCL_OK)
                    return TCL_ERROR;
                break;
            case FOPT_RESULTVAR:
                if (i + 1 >= objc)
                {
                    Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                    return TCL_ERROR;
                }
                resultVar = objv[++i];
                returnRows = 0;
                break;
            case FOPT_RETURNROWS:
                returnRows = 1;
                break;
            case FOPT_ASDICT:
                asDict = 1;
                returnRows = 0;
                break;
        }
    }

    if (!returnRows && maxRows <= 0)
        maxRows = 1;

    uint32_t numCols = 0;
    if (dpiStmt_getNumQueryColumns(st->stmt, &numCols) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getNumQueryColumns");

    if (numCols == 0)
    {
        Tcl_SetObjResult(ip, returnRows ? Tcl_NewListObj(0, NULL) : Tcl_NewIntObj(1403));
        return TCL_OK;
    }

    Tcl_Size numColsSize = (Tcl_Size)numCols;
    Tcl_Obj** colNames = NULL;
    Tcl_Obj** colVals = NULL;
    Tcl_Obj** numberKeys = NULL;
    int* colIsChar = NULL;
    Tcl_Obj* rowsList = NULL;
    Tcl_Obj* rowObj = NULL;
    uint64_t fetched = 0;
    int code = TCL_OK;

    int needNames = (asDict || (dataArray && indexByName));
    size_t colIsCharBytes = 0;
    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(int), &colIsCharBytes, "column metadata") != TCL_OK)
        return TCL_ERROR;
    colIsChar = (int*)Tcl_Alloc(colIsCharBytes);

    if (needNames)
    {
        size_t colNameBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj*), &colNameBytes, "column name array") != TCL_OK)
        {
            code = TCL_ERROR;
            goto cleanup;
        }
        colNames = (Tcl_Obj**)Tcl_Alloc(colNameBytes);
        memset(colNames, 0, colNameBytes);
    }

    for (uint32_t c = 1; c <= numCols; c++)
    {
        dpiQueryInfo qi;
        if (dpiStmt_getQueryInfo(st->stmt, c, &qi) != DPI_SUCCESS)
        {
            code = Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryInfo");
            goto cleanup;
        }
        colIsChar[c - 1] = is_char_type(qi.typeInfo.oracleTypeNum);
        if (needNames)
        {
            colNames[c - 1] = upper_copy(qi.name, qi.nameLength);
            Tcl_IncrRefCount(colNames[c - 1]);
        }
    }

    size_t colValBytes = 0;
    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj*), &colValBytes, "fetched row value array") != TCL_OK)
    {
        code = TCL_ERROR;
        goto cleanup;
    }
    colVals = (Tcl_Obj**)Tcl_Alloc(colValBytes);
    memset(colVals, 0, colValBytes); /* M-2 fix: zero so cleanup can identify populated entries */

    if (dataArray && indexByNumber)
    {
        size_t keyBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj*), &keyBytes, "numeric column keys") != TCL_OK)
        {
            code = TCL_ERROR;
            goto cleanup;
        }
        numberKeys = (Tcl_Obj**)Tcl_Alloc(keyBytes);
        memset(numberKeys, 0, keyBytes);
        for (uint32_t c = 0; c < numCols; c++)
        {
            numberKeys[c] = Tcl_ObjPrintf("%u", c + 1);
            Tcl_IncrRefCount(numberKeys[c]);
        }
    }

    if (returnRows)
    {
        rowsList = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(rowsList);
    }

    for (;;)
    {
        int hasRow = 0;
        uint32_t bufferRowIndex = 0;
        if (dpiStmt_fetch(st->stmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS)
        {
            code = Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_fetch");
            goto cleanup;
        }
        if (!hasRow)
            break;

        /* M-2 fix: zero colVals at the start of each row to prevent stale
         * pointers from the previous iteration (which may have been freed
         * when rowObj was DecrRefCount'd).  On error mid-column, cleanup
         * can then safely identify only the current row's partial objects. */
        memset(colVals, 0, sizeof(Tcl_Obj*) * numCols);

        for (uint32_t c = 1; c <= numCols; c++)
        {
            dpiNativeTypeNum nt;
            dpiData* d = NULL;
            if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS)
            {
                code = Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryValue");
                goto cleanup;
            }
            colVals[c - 1] = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
            if (!colVals[c - 1])
            {
                code = TCL_ERROR;
                goto cleanup;
            }
        }

        rowObj = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(rowObj);
        for (uint32_t c = 0; c < numCols; c++)
        {
            if (asDict)
            {
                if (Tcl_ListObjAppendElement(ip, rowObj, colNames[c]) != TCL_OK ||
                    Tcl_ListObjAppendElement(ip, rowObj, colVals[c]) != TCL_OK)
                {
                    code = TCL_ERROR;
                    goto cleanup;
                }
            }
            else if (Tcl_ListObjAppendElement(ip, rowObj, colVals[c]) != TCL_OK)
            {
                code = TCL_ERROR;
                goto cleanup;
            }
        }

        if (dataArray)
        {
            if (indexByNumber)
            {
                for (uint32_t c = 0; c < numCols; c++)
                {
                    if (!Tcl_ObjSetVar2(ip, dataArray, numberKeys[c], colVals[c], TCL_LEAVE_ERR_MSG))
                    {
                        code = TCL_ERROR;
                        goto cleanup;
                    }
                }
            }
            else if (indexByName)
            {
                for (uint32_t c = 0; c < numCols; c++)
                {
                    if (!Tcl_ObjSetVar2(ip, dataArray, colNames[c], colVals[c], TCL_LEAVE_ERR_MSG))
                    {
                        code = TCL_ERROR;
                        goto cleanup;
                    }
                }
            }
        }

        if (dataVar && !Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG))
        {
            code = TCL_ERROR;
            goto cleanup;
        }

        if (cmd)
        {
            int evalCode = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
            if (evalCode != TCL_OK)
            {
                code = evalCode;
                goto cleanup;
            }
        }

        if (rowsList && Tcl_ListObjAppendElement(ip, rowsList, rowObj) != TCL_OK)
        {
            code = TCL_ERROR;
            goto cleanup;
        }

        Tcl_DecrRefCount(rowObj);
        rowObj = NULL;

        fetched++;
        if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
            break;
    }

    if (resultVar)
    {
        Tcl_Obj* resultObj = rowsList ? rowsList : Tcl_NewListObj(0, NULL);
        if (!Tcl_ObjSetVar2(ip, resultVar, NULL, resultObj, TCL_LEAVE_ERR_MSG))
        {
            code = TCL_ERROR;
            goto cleanup;
        }
    }

    if (returnRows)
    {
        Tcl_SetObjResult(ip, rowsList ? rowsList : Tcl_NewListObj(0, NULL));
        code = TCL_OK;
        goto cleanup;
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(fetched > 0 ? 0 : 1403));
    code = TCL_OK;

cleanup:
    if (rowObj)
        Tcl_DecrRefCount(rowObj);
    if (rowsList)
        Tcl_DecrRefCount(rowsList);
    if (numberKeys)
    {
        for (uint32_t c = 0; c < numCols; c++)
        {
            if (numberKeys[c])
                Tcl_DecrRefCount(numberKeys[c]);
        }
        Tcl_Free((char*)numberKeys);
    }
    if (colVals)
    {
        /* M-2 fix: release any Tcl_Obj* entries that were created by ValueToObj
         * but never adopted by a list (refcount 0 after error mid-column).
         * IncrRefCount+DecrRefCount is safe for both owned (refcount>0) and
         * unowned (refcount 0) objects. */
        for (uint32_t c = 0; c < numCols; c++)
        {
            if (colVals[c])
            {
                Tcl_IncrRefCount(colVals[c]);
                Tcl_DecrRefCount(colVals[c]);
            }
        }
        Tcl_Free((char*)colVals);
    }
    if (colNames)
    {
        for (uint32_t c = 0; c < numCols; c++)
        {
            if (colNames[c])
                Tcl_DecrRefCount(colNames[c]);
        }
        Tcl_Free((char*)colNames);
    }
    if (colIsChar)
        Tcl_Free((char*)colIsChar);
    return code;
}
