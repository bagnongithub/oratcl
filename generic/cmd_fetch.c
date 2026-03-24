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

typedef struct OradpiFetchColMeta
{
    char* name;
    uint32_t nameLen;
    int isChar;
} OradpiFetchColMeta;

typedef struct OradpiFetchCell
{
    int isNull;
    int colIsChar;
    dpiNativeTypeNum nt;
    union
    {
        int64_t i64;
        uint64_t u64;
        float f32;
        double f64;
        int boolean;
        dpiTimestamp ts;
    } scalar;
    char* bytes;
    Tcl_Size bytesLen;
    dpiLob* lob;
} OradpiFetchCell;

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
    Tcl_DStringAppend(&ds, s ? s : "", (Tcl_Size)n);
    Tcl_UtfToUpper(Tcl_DStringValue(&ds));
    Tcl_Obj* o = Tcl_NewStringObj(Tcl_DStringValue(&ds), Tcl_DStringLength(&ds));
    Tcl_DStringFree(&ds);
    return o;
}

static void FreeFetchMeta(OradpiFetchColMeta* meta, Tcl_Size n)
{
    if (!meta)
        return;
    for (Tcl_Size i = 0; i < n; i++)
    {
        if (meta[i].name)
            Tcl_Free(meta[i].name);
    }
    Tcl_Free((char*)meta);
}

static void FreeFetchCells(OradpiFetchCell* cells, Tcl_Size n, GlobalConnRec* shared)
{
    if (!cells)
        return;
    int gated = 0;
    for (Tcl_Size i = 0; i < n; i++)
    {
        if (cells[i].bytes)
        {
            Tcl_Free(cells[i].bytes);
            cells[i].bytes = NULL;
        }
        cells[i].bytesLen = 0;
        if (cells[i].lob)
        {
            if (!gated)
            {
                Oradpi_SharedConnGateEnter(shared);
                gated = 1;
            }
            dpiLob_release(cells[i].lob);
            cells[i].lob = NULL;
        }
    }
    if (gated)
        Oradpi_SharedConnGateLeave(shared);
}

static int SnapshotColumnMeta(Tcl_Interp* ip, OradpiStmt* st, uint32_t numCols, OradpiFetchColMeta* meta)
{
    CONN_GATE_ENTER(st->owner);
    for (uint32_t c = 1; c <= numCols; c++)
    {
        dpiQueryInfo qi;
        size_t nameBytes = 0;
        if (dpiStmt_getQueryInfo(st->stmt, c, &qi) != DPI_SUCCESS)
        {
            CONN_GATE_LEAVE(st->owner);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryInfo");
        }
        meta[c - 1].isChar = is_char_type(qi.typeInfo.oracleTypeNum);
        meta[c - 1].nameLen = qi.nameLength;
        if (qi.nameLength == 0)
            continue;
        if (Oradpi_CheckedAllocBytes(NULL, (Tcl_Size)qi.nameLength + 1, sizeof(char), &nameBytes, "column name copy") != TCL_OK)
        {
            CONN_GATE_LEAVE(st->owner);
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("column name copy is too large"));
            return TCL_ERROR;
        }
        meta[c - 1].name = (char*)Tcl_Alloc(nameBytes);
        memcpy(meta[c - 1].name, qi.name, (size_t)qi.nameLength);
        meta[c - 1].name[qi.nameLength] = '\0';
    }
    CONN_GATE_LEAVE(st->owner);
    return TCL_OK;
}

static int SnapshotCellLocked(OradpiStmt* st,
                              dpiNativeTypeNum nt,
                              dpiData* d,
                              int colIsChar,
                              OradpiFetchCell* cell,
                              const char** odpiWhereOut,
                              const char** msgOut)
{
    memset(cell, 0, sizeof(*cell));
    cell->nt = nt;
    cell->colIsChar = colIsChar;

    if (odpiWhereOut)
        *odpiWhereOut = NULL;
    if (msgOut)
        *msgOut = NULL;

    if (!d || d->isNull)
    {
        cell->isNull = 1;
        return TCL_OK;
    }

    switch (nt)
    {
        case DPI_NATIVE_TYPE_INT64:
            cell->scalar.i64 = d->value.asInt64;
            return TCL_OK;
        case DPI_NATIVE_TYPE_UINT64:
            cell->scalar.u64 = d->value.asUint64;
            return TCL_OK;
        case DPI_NATIVE_TYPE_FLOAT:
            cell->scalar.f32 = d->value.asFloat;
            return TCL_OK;
        case DPI_NATIVE_TYPE_DOUBLE:
            cell->scalar.f64 = d->value.asDouble;
            return TCL_OK;
        case DPI_NATIVE_TYPE_BOOLEAN:
            cell->scalar.boolean = d->value.asBoolean ? 1 : 0;
            return TCL_OK;
        case DPI_NATIVE_TYPE_TIMESTAMP:
            cell->scalar.ts = d->value.asTimestamp;
            return TCL_OK;
        case DPI_NATIVE_TYPE_BYTES:
        {
            const dpiBytes* b = &d->value.asBytes;
            size_t bufBytes = 0;
            if (!b->ptr || b->length == 0)
                return TCL_OK;
            if (Oradpi_CheckedAllocBytes(NULL, (Tcl_Size)b->length, sizeof(char), &bufBytes, "fetched byte value") != TCL_OK)
            {
                if (msgOut)
                    *msgOut = "fetched byte value is too large";
                return TCL_ERROR;
            }
            cell->bytes = (char*)Tcl_Alloc(bufBytes);
            memcpy(cell->bytes, b->ptr, (size_t)b->length);
            cell->bytesLen = (Tcl_Size)b->length;
            return TCL_OK;
        }
        case DPI_NATIVE_TYPE_LOB:
        {
            OradpiConn* co = st->owner;
            dpiLob* lob = d->value.asLOB;
            if (!lob)
                return TCL_OK;

            if (co && co->inlineLobs == 0)
            {
                if (dpiLob_addRef(lob) != DPI_SUCCESS)
                {
                    if (odpiWhereOut)
                        *odpiWhereOut = "dpiLob_addRef";
                    return TCL_ERROR;
                }
                cell->lob = lob;
                return TCL_OK;
            }

            uint64_t sizeCharsOrBytes = 0;
            uint64_t capBytes = 0;
            uint64_t gotBytes = 0;
            size_t bufBytes = 0;
            if (dpiLob_getSize(lob, &sizeCharsOrBytes) != DPI_SUCCESS)
            {
                if (odpiWhereOut)
                    *odpiWhereOut = "dpiLob_getSize";
                return TCL_ERROR;
            }
            if (sizeCharsOrBytes == 0)
                return TCL_OK;
            if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS)
            {
                if (odpiWhereOut)
                    *odpiWhereOut = "dpiLob_getBufferSize";
                return TCL_ERROR;
            }

            const uint64_t MAX_INLINE = (1u << 20);
            if (capBytes > MAX_INLINE && capBytes > 0)
            {
                if (msgOut)
                    *msgOut =
                        "LOB value exceeds 1 MB inline limit; disable inlineLobs and use oralob read -amount for large values";
                return TCL_ERROR;
            }
            if (Oradpi_CheckedU64ToSizeT(NULL, capBytes, &bufBytes, "inline LOB buffer") != TCL_OK)
            {
                if (msgOut)
                    *msgOut = "inline LOB buffer is too large";
                return TCL_ERROR;
            }
            if (bufBytes == 0)
                return TCL_OK;

            cell->bytes = (char*)Tcl_Alloc(bufBytes);
            gotBytes = capBytes;
            if (dpiLob_readBytes(lob, 1, sizeCharsOrBytes, cell->bytes, &gotBytes) != DPI_SUCCESS)
            {
                Tcl_Free(cell->bytes);
                cell->bytes = NULL;
                if (odpiWhereOut)
                    *odpiWhereOut = "dpiLob_readBytes";
                return TCL_ERROR;
            }
            cell->bytesLen = (Tcl_Size)gotBytes;
            return TCL_OK;
        }
        default:
            return TCL_OK;
    }
}

static Tcl_Obj* SnapshotCellToObj(Tcl_Interp* ip, OradpiStmt* st, OradpiFetchCell* cell)
{
    if (!cell || cell->isNull)
        return Tcl_NewObj();

    switch (cell->nt)
    {
        case DPI_NATIVE_TYPE_INT64:
        {
            long long v = cell->scalar.i64;
            if (v >= INT_MIN && v <= INT_MAX)
                return Tcl_NewIntObj((int)v);
            return Tcl_NewWideIntObj((Tcl_WideInt)v);
        }
        case DPI_NATIVE_TYPE_UINT64:
        {
            uint64_t uv = cell->scalar.u64;
            if (uv <= (uint64_t)INT64_MAX)
                return Tcl_NewWideIntObj((Tcl_WideInt)uv);
            return Tcl_ObjPrintf("%" PRIu64, uv);
        }
        case DPI_NATIVE_TYPE_FLOAT:
        {
            double dv = (double)cell->scalar.f32;
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
            double dv = cell->scalar.f64;
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
            return Tcl_NewBooleanObj(cell->scalar.boolean ? 1 : 0);
        case DPI_NATIVE_TYPE_TIMESTAMP:
        {
            const dpiTimestamp* ts = &cell->scalar.ts;
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
            if (!cell->bytes || cell->bytesLen == 0)
                return Tcl_NewObj();
            return cell->colIsChar ? Tcl_NewStringObj(cell->bytes, cell->bytesLen)
                                   : Tcl_NewByteArrayObj((const unsigned char*)cell->bytes, cell->bytesLen);
        case DPI_NATIVE_TYPE_LOB:
            if (cell->lob)
            {
                OradpiLob* L = Oradpi_NewLob(ip, cell->lob, st->owner ? st->owner->shared : NULL);
                cell->lob = NULL; /* ownership transferred to the wrapper */
                return L->base.name;
            }
            if (!cell->bytes || cell->bytesLen == 0)
                return Tcl_NewObj();
            return cell->colIsChar ? Tcl_NewStringObj(cell->bytes, cell->bytesLen)
                                   : Tcl_NewByteArrayObj((const unsigned char*)cell->bytes, cell->bytesLen);
        default:
            return Tcl_NewObj();
    }
}

/*
 * orafetch statement-handle ?-datavariable varName? ?-dataarray arrName?
 *         ?-indexbyname? ?-indexbynumber? ?-command script? ?-max N?
 *         ?-resultvariable varName? ?-returnrows? ?-asdict?
 *
 *   Fetches rows from a previously executed query. By default returns 0
 *   while rows remain and 1403 at end-of-data. Use -returnrows or
 *   -resultvariable to collect row lists. Options control variable binding,
 *   per-row callbacks, and result format (dict, array, etc.).
 *   Returns: 0 (rows fetched), 1403 (no data found), or a row list when
 *   -returnrows is used.
 *   Errors:  ODPI-C fetch errors; invalid handle; async busy (errors immediately).
 *   Thread-safety: safe — per-interp state only.
 */
int Oradpi_Cmd_Fetch(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    Tcl_Obj* dataVar = NULL;
    Tcl_Obj* dataArray = NULL;
    int indexByName = 0, indexByNumber = 0;
    Tcl_Obj* cmd = NULL;
    Tcl_WideInt maxRows = 0;
    Tcl_Obj* resultVar = NULL;
    /* V-8 fix: default to status-code mode (0 / 1403) as documented.
     * The old default of 1 caused plain "orafetch $S" to materialize the
     * entire result set into a list, contradicting the docs and risking
     * OOM on large queries.  -returnrows explicitly enables list mode. */
    int returnRows = 0;
    int asDict = 0;

    uint32_t numCols = 0;
    Tcl_Size numColsSize = 0;
    OradpiFetchColMeta* meta = NULL;
    OradpiFetchCell* cells = NULL;
    Tcl_Obj** colNames = NULL;
    Tcl_Obj** colVals = NULL;
    Tcl_Obj** numberKeys = NULL;
    Tcl_Obj* rowsList = NULL;
    Tcl_Obj* rowObj = NULL;
    uint64_t fetched = 0;
    int code = TCL_OK;
    int needNames = 0;
    size_t metaBytes = 0;
    size_t cellBytes = 0;
    size_t colNameBytes = 0;
    size_t colValBytes = 0;

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

    if (maxRows < 0)
        return Oradpi_SetError(ip, (OradpiBase*)st, -1, "orafetch: -max must be >= 0");
    /* Default to single-row fetch only in plain status-code mode.
     * When -command, -resultvar, or -returnrows is active, fetch all rows
     * unless the caller explicitly provides -max.  Note: this means
     * "orafetch $S -command {…}" on a large result set will iterate all
     * rows — callers should use -max to limit if the table is large. */
    if (!returnRows && !cmd && !resultVar && maxRows == 0)
        maxRows = 1;

    CONN_GATE_ENTER(st->owner);
    if (dpiStmt_getNumQueryColumns(st->stmt, &numCols) != DPI_SUCCESS)
    {
        CONN_GATE_LEAVE(st->owner);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getNumQueryColumns");
    }
    CONN_GATE_LEAVE(st->owner);

    if (numCols == 0)
    {
        Tcl_SetObjResult(ip, returnRows ? Tcl_NewListObj(0, NULL) : Tcl_NewIntObj(1403));
        return TCL_OK;
    }

    numColsSize = (Tcl_Size)numCols;
    needNames = (asDict || (dataArray && indexByName));

    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(*meta), &metaBytes, "column metadata snapshot") != TCL_OK)
        return TCL_ERROR;
    meta = (OradpiFetchColMeta*)Tcl_Alloc(metaBytes);
    memset(meta, 0, metaBytes);
    if (SnapshotColumnMeta(ip, st, numCols, meta) != TCL_OK)
    {
        code = TCL_ERROR;
        goto cleanup;
    }

    if (needNames)
    {
        if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj*), &colNameBytes, "column name array") != TCL_OK)
        {
            code = TCL_ERROR;
            goto cleanup;
        }
        colNames = (Tcl_Obj**)Tcl_Alloc(colNameBytes);
        memset(colNames, 0, colNameBytes);
        for (uint32_t c = 0; c < numCols; c++)
        {
            colNames[c] = meta[c].nameLen ? upper_copy(meta[c].name, meta[c].nameLen) : Tcl_NewStringObj("", 0);
            Tcl_IncrRefCount(colNames[c]);
        }
    }

    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj*), &colValBytes, "fetched row value array") != TCL_OK)
    {
        code = TCL_ERROR;
        goto cleanup;
    }
    colVals = (Tcl_Obj**)Tcl_Alloc(colValBytes);
    memset(colVals, 0, colValBytes);

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
            numberKeys[c] = Tcl_ObjPrintf("%u", c);
            Tcl_IncrRefCount(numberKeys[c]);
        }
    }

    if (returnRows || resultVar)
    {
        rowsList = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(rowsList);
    }

    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(*cells), &cellBytes, "fetched row snapshot array") != TCL_OK)
    {
        code = TCL_ERROR;
        goto cleanup;
    }
    cells = (OradpiFetchCell*)Tcl_Alloc(cellBytes);
    memset(cells, 0, cellBytes);

    for (;;)
    {
        int hasRow = 0;
        uint32_t bufferRowIndex = 0;
        const char* snapshotWhere = NULL;
        const char* snapshotMsg = NULL;

        memset(colVals, 0, colValBytes);
        FreeFetchCells(cells, numColsSize, st->owner ? st->owner->shared : NULL);
        memset(cells, 0, cellBytes);

        CONN_GATE_ENTER(st->owner);
        if (dpiStmt_fetch(st->stmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS)
        {
            CONN_GATE_LEAVE(st->owner);
            code = Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_fetch");
            goto cleanup;
        }
        (void)bufferRowIndex;
        if (!hasRow)
        {
            CONN_GATE_LEAVE(st->owner);
            break;
        }

        for (uint32_t c = 1; c <= numCols; c++)
        {
            dpiNativeTypeNum nt;
            dpiData* d = NULL;
            if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS)
            {
                CONN_GATE_LEAVE(st->owner);
                code = Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, "dpiStmt_getQueryValue");
                goto cleanup;
            }
            if (SnapshotCellLocked(st, nt, d, meta[c - 1].isChar, &cells[c - 1], &snapshotWhere, &snapshotMsg) != TCL_OK)
            {
                CONN_GATE_LEAVE(st->owner);
                if (snapshotWhere)
                    code = Oradpi_SetErrorFromODPI(ip, (OradpiBase*)st, snapshotWhere);
                else
                {
                    Tcl_SetObjResult(ip, Tcl_NewStringObj(snapshotMsg ? snapshotMsg : "failed to snapshot fetched value", -1));
                    code = TCL_ERROR;
                }
                goto cleanup;
            }
        }
        CONN_GATE_LEAVE(st->owner);

        for (uint32_t c = 0; c < numCols; c++)
        {
            colVals[c] = SnapshotCellToObj(ip, st, &cells[c]);
            if (!colVals[c])
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
                LAPPEND_GOTO(ip, rowObj, colNames[c], code, cleanup);
                LAPPEND_GOTO(ip, rowObj, colVals[c], code, cleanup);
            }
            else
            {
                LAPPEND_GOTO(ip, rowObj, colVals[c], code, cleanup);
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
            if (evalCode == TCL_BREAK)
            {
                /* TCL_BREAK: stop fetching, treat as normal completion */
                Tcl_DecrRefCount(rowObj);
                rowObj = NULL;
                memset(colVals, 0, colValBytes);
                fetched++;
                break;
            }
            if (evalCode == TCL_CONTINUE)
            {
                /* TCL_CONTINUE: skip to next row (don't append to rowsList) */
                Tcl_DecrRefCount(rowObj);
                rowObj = NULL;
                memset(colVals, 0, colValBytes);
                fetched++;
                if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
                    break;
                continue;
            }
            if (evalCode != TCL_OK)
            {
                code = evalCode;
                goto cleanup;
            }
        }

        if (rowsList)
        {
            LAPPEND_GOTO(ip, rowsList, rowObj, code, cleanup);
        }

        Tcl_DecrRefCount(rowObj);
        rowObj = NULL;
        /* Freeing rowObj may have freed the colVals[] elements it owned.
         * Zero the array so cleanup never touches dangling pointers. */
        memset(colVals, 0, colValBytes);

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
        /* colVals[] entries are owned by rowObj or rowsList — they are
         * released when those containers are freed above.  Any surviving
         * non-NULL entry here still has a live owner; we must not touch
         * its refcount.  Just free the pointer array itself. */
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
    if (cells)
    {
        FreeFetchCells(cells, numColsSize, st->owner ? st->owner->shared : NULL);
        Tcl_Free((char*)cells);
    }
    FreeFetchMeta(meta, numColsSize);
    return code;
}
