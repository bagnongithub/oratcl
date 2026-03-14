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
 * Stuff
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

static Tcl_Obj* upper_copy(const char* s, uint32_t n)
{
    /* Build uppercased string in a stack buffer to avoid mutating
     * Tcl_Obj's internal string rep in-place (fix 3.1) */
    char stack[256];
    char* buf = (n < sizeof(stack)) ? stack : (char*)Tcl_Alloc(n + 1);
    for (uint32_t i = 0; i < n; i++)
    {
        char ch = s[i];
        buf[i] = (ch >= 'a' && ch <= 'z') ? (char)(ch - 'a' + 'A') : ch;
    }
    buf[n] = '\0';
    Tcl_Obj* o = Tcl_NewStringObj(buf, (Tcl_Size)n);
    if (buf != stack)
        Tcl_Free(buf);
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
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_getSize"), NULL;
            if (sizeCharsOrBytes == 0)
                return Tcl_NewObj();

            uint64_t capBytes = 0;
            if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_getBufferSize"), NULL;

            const uint64_t MAX_INLINE = (1u << 20);
            if (capBytes > MAX_INLINE && capBytes > 0)
            {
                uint64_t scaledChars = (sizeCharsOrBytes * MAX_INLINE) / capBytes;
                if (scaledChars == 0)
                    scaledChars = 1;
                sizeCharsOrBytes = scaledChars;
                if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS)
                    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_getBufferSize"), NULL;
            }

            char* buf = (char*)Tcl_Alloc((size_t)capBytes);
            uint64_t gotBytes = capBytes;
            if (dpiLob_readBytes(lob, 1, sizeCharsOrBytes, buf, &gotBytes) != DPI_SUCCESS)
            {
                Tcl_Free(buf);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiLob_readBytes"), NULL;
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
    {
        maxRows = 1;
    }

    uint32_t numCols = 0;
    if (dpiStmt_getNumQueryColumns(st->stmt, &numCols) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getNumQueryColumns");

    if (numCols == 0)
    {
        Tcl_SetObjResult(ip, returnRows ? Tcl_NewListObj(0, NULL) : Tcl_NewIntObj(1403));
        return TCL_OK;
    }

    Tcl_Obj** colNames = NULL;
    int needNames = (asDict || (dataArray && indexByName));
    int* colIsChar = (int*)Tcl_Alloc(numCols * sizeof(int));
    if (needNames)
        colNames = (Tcl_Obj**)Tcl_Alloc(numCols * sizeof(Tcl_Obj*));
    /* Single-pass column metadata (fix 4.4) */
    for (uint32_t c = 1; c <= numCols; c++)
    {
        dpiQueryInfo qi;
        if (dpiStmt_getQueryInfo(st->stmt, c, &qi) != DPI_SUCCESS)
        {
            /* Clean up any colNames already allocated (fix 2.5 partial alloc) */
            if (colNames)
            {
                for (uint32_t k = 1; k < c; k++)
                    Tcl_DecrRefCount(colNames[k - 1]);
                Tcl_Free((char*)colNames);
            }
            Tcl_Free((char*)colIsChar);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryInfo");
        }
        colIsChar[c - 1] = is_char_type(qi.typeInfo.oracleTypeNum);
        if (needNames)
        {
            colNames[c - 1] = upper_copy(qi.name, qi.nameLength);
            Tcl_IncrRefCount(colNames[c - 1]);
        }
    }

    if (!returnRows && maxRows == 1)
    {
        int hasRow = 0;
        uint32_t bufferRowIndex = 0;
        if (dpiStmt_fetch(st->stmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS)
        {
            if (colNames)
            {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free((char*)colNames);
            }
            Tcl_Free((char*)colIsChar);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_fetch");
        }
        if (!hasRow)
        {
            if (colNames)
            {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free((char*)colNames);
            }
            Tcl_Free((char*)colIsChar);
            Tcl_SetObjResult(ip, Tcl_NewIntObj(1403));
            return TCL_OK;
        }
        Tcl_Obj* rowObj = Tcl_NewListObj(0, NULL);
        for (uint32_t c = 1; c <= numCols; c++)
        {
            dpiNativeTypeNum nt;
            dpiData* d = NULL;
            if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS)
            {
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryValue");
            }
            Tcl_Obj* v = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
            if (!v)
            {
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                return TCL_ERROR;
            }
            if (asDict)
            {
                Tcl_ListObjAppendElement(ip, rowObj, colNames[c - 1]);
                Tcl_ListObjAppendElement(ip, rowObj, v);
            }
            else
            {
                Tcl_ListObjAppendElement(ip, rowObj, v);
            }
        }
        if (dataVar && !Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG))
        {
            if (colNames)
            {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free((char*)colNames);
            }
            Tcl_Free((char*)colIsChar);
            return TCL_ERROR;
        }
        if (cmd)
        {
            Tcl_IncrRefCount(rowObj);
            int code = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
            Tcl_DecrRefCount(rowObj);
            if (code != TCL_OK)
            {
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                return code;
            }
        }
        if (colNames)
        {
            for (uint32_t i = 0; i < numCols; i++)
                Tcl_DecrRefCount(colNames[i]);
            Tcl_Free((char*)colNames);
        }
        Tcl_Free((char*)colIsChar);
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    Tcl_Obj* rowsList = NULL;
    if (returnRows)
    {
        rowsList = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(rowsList);
    }

    uint64_t fetched = 0;
    /* Allocate colVals once outside loop to avoid per-row malloc/free (fix 4.1) */
    Tcl_Obj** colVals = (Tcl_Obj**)Tcl_Alloc(numCols * sizeof(Tcl_Obj*));
    for (;;)
    {
        int hasRow = 0;
        uint32_t bufferRowIndex = 0;
        if (dpiStmt_fetch(st->stmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS)
        {
            Tcl_Free((char*)colVals);
            if (colNames)
            {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free((char*)colNames);
            }
            Tcl_Free((char*)colIsChar);
            if (rowsList)
                Tcl_DecrRefCount(rowsList);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_fetch");
        }
        if (!hasRow)
            break;

        /* Extract all column values ONCE per row */
        for (uint32_t c = 1; c <= numCols; c++)
        {
            dpiNativeTypeNum nt;
            dpiData* d = NULL;
            if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS)
            {
                Tcl_Free((char*)colVals);
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryValue");
            }
            colVals[c - 1] = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
            if (!colVals[c - 1])
            {
                Tcl_Free((char*)colVals);
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return TCL_ERROR;
            }
        }

        /* Build rowObj from extracted values */
        Tcl_Obj* rowObj = Tcl_NewListObj(0, NULL);
        for (uint32_t c = 0; c < numCols; c++)
        {
            if (asDict)
                Tcl_ListObjAppendElement(ip, rowObj, colNames[c]);
            Tcl_ListObjAppendElement(ip, rowObj, colVals[c]);
        }

        /* Populate dataArray from the same extracted values (no re-fetch) */
        if (dataArray)
        {
            if (indexByNumber)
            {
                for (uint32_t c = 0; c < numCols; c++)
                {
                    Tcl_Obj* keyObj = Tcl_ObjPrintf("%u", c + 1);
                    Tcl_IncrRefCount(keyObj);
                    if (!Tcl_ObjSetVar2(ip, dataArray, keyObj, colVals[c], TCL_LEAVE_ERR_MSG))
                    {
                        Tcl_DecrRefCount(keyObj);
                        Tcl_Free((char*)colVals);
                        if (colNames)
                        {
                            for (uint32_t i = 0; i < numCols; i++)
                                Tcl_DecrRefCount(colNames[i]);
                            Tcl_Free((char*)colNames);
                        }
                        Tcl_Free((char*)colIsChar);
                        if (rowsList)
                            Tcl_DecrRefCount(rowsList);
                        return TCL_ERROR;
                    }
                    Tcl_DecrRefCount(keyObj);
                }
            }
            else if (indexByName)
            {
                for (uint32_t c = 0; c < numCols; c++)
                {
                    if (!Tcl_ObjSetVar2(ip, dataArray, colNames[c], colVals[c], TCL_LEAVE_ERR_MSG))
                    {
                        Tcl_Free((char*)colVals);
                        if (colNames)
                        {
                            for (uint32_t i = 0; i < numCols; i++)
                                Tcl_DecrRefCount(colNames[i]);
                            Tcl_Free((char*)colNames);
                        }
                        Tcl_Free((char*)colIsChar);
                        if (rowsList)
                            Tcl_DecrRefCount(rowsList);
                        return TCL_ERROR;
                    }
                }
            }
        }

        if (dataVar)
        {
            if (!Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG))
            {
                Tcl_Free((char*)colVals);
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return TCL_ERROR;
            }
        }
        if (cmd)
        {
            Tcl_IncrRefCount(rowObj);
            int code = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
            Tcl_DecrRefCount(rowObj);
            if (code != TCL_OK)
            {
                Tcl_Free((char*)colVals);
                if (colNames)
                {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free((char*)colNames);
                }
                Tcl_Free((char*)colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return code;
            }
        }
        if (rowsList)
        {
            Tcl_ListObjAppendElement(ip, rowsList, rowObj);
        }

        fetched++;
        if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
            break;
    }

    Tcl_Free((char*)colVals);
    if (colNames)
    {
        for (uint32_t i = 0; i < numCols; i++)
            Tcl_DecrRefCount(colNames[i]);
        Tcl_Free((char*)colNames);
    }
    Tcl_Free((char*)colIsChar);

    /* Fix 3.5: Check resultVar set return and handle rowsList lifetime correctly */
    if (resultVar)
    {
        if (!Tcl_ObjSetVar2(ip, resultVar, NULL, rowsList ? rowsList : Tcl_NewListObj(0, NULL), TCL_LEAVE_ERR_MSG))
        {
            if (rowsList)
                Tcl_DecrRefCount(rowsList);
            return TCL_ERROR;
        }
    }
    if (returnRows)
    {
        Tcl_SetObjResult(ip, rowsList ? rowsList : Tcl_NewListObj(0, NULL));
        if (rowsList)
            Tcl_DecrRefCount(rowsList);
        return TCL_OK;
    }
    if (rowsList)
        Tcl_DecrRefCount(rowsList);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(fetched > 0 ? 0 : 1403));
    return TCL_OK;
}
