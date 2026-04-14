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

static int      is_char_type(dpiOracleTypeNum otn);
int             Oradpi_Cmd_Fetch(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]);
static Tcl_Obj *upper_copy(const char *s, uint32_t n);

typedef struct OradpiFetchColMeta {
    char            *name;
    uint32_t         nameLen;
    int              isChar;
    /* Type fields needed to create an explicit dpiVar per column. */
    dpiOracleTypeNum oracleTypeNum;
    dpiNativeTypeNum defaultNativeTypeNum;
    uint32_t         clientSizeInBytes;
} OradpiFetchColMeta;

typedef struct OradpiFetchCell {
    int              isNull;
    int              colIsChar;
    dpiNativeTypeNum nt;
    union {
        int64_t      i64;
        uint64_t     u64;
        float        f32;
        double       f64;
        int          boolean;
        dpiTimestamp ts;
    } scalar;
    char    *bytes;
    Tcl_Size bytesLen;
    dpiLob  *lob;
} OradpiFetchCell;

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

static int is_char_type(dpiOracleTypeNum otn) {
    switch (otn) {
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

/* Use Tcl_UtfToUpper for Unicode-aware uppercasing of column names.
 * Oracle column names may contain accented characters in NLS configurations. */
static Tcl_Obj *upper_copy(const char *s, uint32_t n) {
    Tcl_DString ds;
    Tcl_DStringInit(&ds);
    Tcl_DStringAppend(&ds, s ? s : "", (Tcl_Size)n);
    Tcl_UtfToUpper(Tcl_DStringValue(&ds));
    Tcl_Obj *o = Tcl_NewStringObj(Tcl_DStringValue(&ds), Tcl_DStringLength(&ds));
    Tcl_DStringFree(&ds);
    return o;
}

static void FreeFetchMeta(OradpiFetchColMeta *meta, Tcl_Size n) {
    if (!meta)
        return;
    for (Tcl_Size i = 0; i < n; i++) {
        if (meta[i].name)
            Tcl_Free(meta[i].name);
    }
    Tcl_Free((char *)meta);
}

static void FreeFetchCells(OradpiFetchCell *cells, Tcl_Size n, GlobalConnRec *shared) {
    if (!cells)
        return;
    int gated = 0;
    for (Tcl_Size i = 0; i < n; i++) {
        if (cells[i].bytes) {
            Tcl_Free(cells[i].bytes);
            cells[i].bytes = NULL;
        }
        cells[i].bytesLen = 0;
        if (cells[i].lob) {
            if (!gated) {
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

static int SnapshotColumnMeta(Tcl_Interp *ip, OradpiStmt *st, uint32_t numCols, OradpiFetchColMeta *meta) {
    CONN_GATE_ENTER(st->owner);
    for (uint32_t c = 1; c <= numCols; c++) {
        dpiQueryInfo qi;
        size_t       nameBytes = 0;
        if (dpiStmt_getQueryInfo(st->stmt, c, &qi) != DPI_SUCCESS) {
            CONN_GATE_LEAVE(st->owner);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryInfo");
        }
        meta[c - 1].isChar               = is_char_type(qi.typeInfo.oracleTypeNum);
        meta[c - 1].oracleTypeNum        = qi.typeInfo.oracleTypeNum;
        meta[c - 1].defaultNativeTypeNum = qi.typeInfo.defaultNativeTypeNum;
        meta[c - 1].clientSizeInBytes    = qi.typeInfo.clientSizeInBytes;
        meta[c - 1].nameLen              = qi.nameLength;
        if (qi.nameLength == 0)
            continue;
        if (Oradpi_CheckedAllocBytes(NULL, (Tcl_Size)qi.nameLength + 1, sizeof(char), &nameBytes, "column name copy") != TCL_OK) {
            CONN_GATE_LEAVE(st->owner);
            Tcl_SetObjResult(ip, Tcl_ObjPrintf("column name copy is too large"));
            return TCL_ERROR;
        }
        meta[c - 1].name = (char *)Tcl_Alloc(nameBytes);
        memcpy(meta[c - 1].name, qi.name, (size_t)qi.nameLength);
        meta[c - 1].name[qi.nameLength] = '\0';
    }
    CONN_GATE_LEAVE(st->owner);
    return TCL_OK;
}

static int SnapshotCellLocked(int inlineLobs, dpiNativeTypeNum nt, dpiData *d, int colIsChar, OradpiFetchCell *cell, const char **odpiWhereOut, const char **msgOut) {
    memset(cell, 0, sizeof(*cell));
    cell->nt        = nt;
    cell->colIsChar = colIsChar;

    if (odpiWhereOut)
        *odpiWhereOut = NULL;
    if (msgOut)
        *msgOut = NULL;

    if (!d || d->isNull) {
        cell->isNull = 1;
        return TCL_OK;
    }

    switch (nt) {
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
    case DPI_NATIVE_TYPE_BYTES: {
        const dpiBytes *b        = &d->value.asBytes;
        size_t          bufBytes = 0;
        if (!b->ptr || b->length == 0)
            return TCL_OK;
        if (Oradpi_CheckedAllocBytes(NULL, (Tcl_Size)b->length, sizeof(char), &bufBytes, "fetched byte value") != TCL_OK) {
            if (msgOut)
                *msgOut = "fetched byte value is too large";
            return TCL_ERROR;
        }
        cell->bytes = (char *)Tcl_Alloc(bufBytes);
        memcpy(cell->bytes, b->ptr, (size_t)b->length);
        cell->bytesLen = (Tcl_Size)b->length;
        return TCL_OK;
    }
    case DPI_NATIVE_TYPE_LOB: {
        /* Use snapshotted inlineLobs instead of st->owner->inlineLobs
         * to avoid dereferencing the statement wrapper which may have been
         * freed by a reentrancy callback. */
        dpiLob *lob = d->value.asLOB;
        if (!lob)
            return TCL_OK;

        if (inlineLobs == 0) {
            if (dpiLob_addRef(lob) != DPI_SUCCESS) {
                if (odpiWhereOut)
                    *odpiWhereOut = "dpiLob_addRef";
                return TCL_ERROR;
            }
            cell->lob = lob;
            return TCL_OK;
        }

        uint64_t sizeCharsOrBytes = 0;
        uint64_t capBytes         = 0;
        uint64_t gotBytes         = 0;
        size_t   bufBytes         = 0;
        if (dpiLob_getSize(lob, &sizeCharsOrBytes) != DPI_SUCCESS) {
            if (odpiWhereOut)
                *odpiWhereOut = "dpiLob_getSize";
            return TCL_ERROR;
        }
        if (sizeCharsOrBytes == 0)
            return TCL_OK;
        if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS) {
            if (odpiWhereOut)
                *odpiWhereOut = "dpiLob_getBufferSize";
            return TCL_ERROR;
        }

        const uint64_t MAX_INLINE = (1u << 20);
        if (capBytes > MAX_INLINE && capBytes > 0) {
            if (msgOut)
                *msgOut = "LOB value exceeds 1 MB inline limit; disable inlineLobs and use oralob read -amount for large values";
            return TCL_ERROR;
        }
        if (Oradpi_CheckedU64ToSizeT(NULL, capBytes, &bufBytes, "inline LOB buffer") != TCL_OK) {
            if (msgOut)
                *msgOut = "inline LOB buffer is too large";
            return TCL_ERROR;
        }
        if (bufBytes == 0)
            return TCL_OK;

        cell->bytes = (char *)Tcl_Alloc(bufBytes);
        gotBytes    = capBytes;
        if (dpiLob_readBytes(lob, 1, sizeCharsOrBytes, cell->bytes, &gotBytes) != DPI_SUCCESS) {
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

static Tcl_Obj *SnapshotCellToObj(Tcl_Interp *ip, GlobalConnRec *shared, OradpiFetchCell *cell) {
    if (!cell || cell->isNull)
        return Tcl_NewObj();

    switch (cell->nt) {
    case DPI_NATIVE_TYPE_INT64: {
        long long v = cell->scalar.i64;
        if (v >= INT_MIN && v <= INT_MAX)
            return Tcl_NewIntObj((int)v);
        return Tcl_NewWideIntObj((Tcl_WideInt)v);
    }
    case DPI_NATIVE_TYPE_UINT64: {
        uint64_t uv = cell->scalar.u64;
        if (uv <= (uint64_t)INT64_MAX)
            return Tcl_NewWideIntObj((Tcl_WideInt)uv);
        return Tcl_ObjPrintf("%" PRIu64, uv);
    }
    case DPI_NATIVE_TYPE_FLOAT: {
        double dv = (double)cell->scalar.f32;
        if (isfinite(dv)) {
            double intpart;
            if (modf(dv, &intpart) == 0.0) {
                if (intpart >= (double)INT_MIN && intpart <= (double)INT_MAX)
                    return Tcl_NewIntObj((int)intpart);
                if (intpart >= (double)LLONG_MIN && intpart <= (double)LLONG_MAX)
                    return Tcl_NewWideIntObj((Tcl_WideInt)((long long)intpart));
            }
        }
        return Tcl_NewDoubleObj(dv);
    }
    case DPI_NATIVE_TYPE_DOUBLE: {
        double dv = cell->scalar.f64;
        if (isfinite(dv)) {
            double intpart;
            if (modf(dv, &intpart) == 0.0) {
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
    case DPI_NATIVE_TYPE_TIMESTAMP: {
        const dpiTimestamp *ts = &cell->scalar.ts;
        return Tcl_ObjPrintf("%04d-%02u-%02uT%02u:%02u:%02u.%06u", ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second, ts->fsecond / 1000);
    }
    case DPI_NATIVE_TYPE_BYTES:
        if (!cell->bytes || cell->bytesLen == 0)
            return Tcl_NewObj();
        return cell->colIsChar ? Tcl_NewStringObj(cell->bytes, cell->bytesLen) : Tcl_NewByteArrayObj((const unsigned char *)cell->bytes, cell->bytesLen);
    case DPI_NATIVE_TYPE_LOB:
        if (cell->lob) {
            /* Use snapshotted shared instead of st->owner->shared */
            OradpiLob *L = Oradpi_NewLob(ip, cell->lob, shared);
            cell->lob    = NULL; /* ownership transferred to the wrapper */
            return L->base.name;
        }
        if (!cell->bytes || cell->bytesLen == 0)
            return Tcl_NewObj();
        return cell->colIsChar ? Tcl_NewStringObj(cell->bytes, cell->bytesLen) : Tcl_NewByteArrayObj((const unsigned char *)cell->bytes, cell->bytesLen);
    default:
        return Tcl_NewObj();
    }
}

/* Invalidate and free the per-statement fetch metadata cache.
 * Called on re-parse (oraparse / orasql / oraplexec) and at statement
 * teardown (Oradpi_FreeStmt).  Safe to call on a statement with no cache. */
void Oradpi_FreeFetchCache(OradpiStmt *s) {
    if (!s || s->fetchCacheNumCols == 0)
        return;
    uint32_t n = s->fetchCacheNumCols;
    if (s->fetchIsChar) {
        Tcl_Free((char *)s->fetchIsChar);
        s->fetchIsChar = NULL;
    }
    if (s->fetchColNames) {
        for (uint32_t c = 0; c < n; c++)
            if (s->fetchColNames[c])
                Tcl_DecrRefCount(s->fetchColNames[c]);
        Tcl_Free((char *)s->fetchColNames);
        s->fetchColNames = NULL;
    }
    if (s->fetchNumberKeys) {
        for (uint32_t c = 0; c < n; c++)
            if (s->fetchNumberKeys[c])
                Tcl_DecrRefCount(s->fetchNumberKeys[c]);
        Tcl_Free((char *)s->fetchNumberKeys);
        s->fetchNumberKeys = NULL;
    }
    /* Release pre-defined output variables. */
    if (s->fetchVars) {
        for (uint32_t c = 0; c < n; c++)
            if (s->fetchVars[c])
                dpiVar_release(s->fetchVars[c]);
        Tcl_Free((char *)s->fetchVars);
        s->fetchVars = NULL;
        Tcl_Free((char *)s->fetchVarData);
        s->fetchVarData = NULL;
        Tcl_Free((char *)s->fetchNativeTypes);
        s->fetchNativeTypes = NULL;
    }
    s->fetchCacheNumCols = 0;
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
int Oradpi_Cmd_Fetch(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    Tcl_Obj            *dataVar     = NULL;
    Tcl_Obj            *dataArray   = NULL;
    int                 indexByName = 0, indexByNumber = 0;
    Tcl_Obj            *cmd             = NULL;
    Tcl_WideInt         maxRows         = 0;
    Tcl_Obj            *resultVar       = NULL;
    /* default to status-code mode (0 / 1403) as documented.
     * Default to status-code mode (0 / 1403).  -returnrows explicitly
     * enables list-return mode.  Plain "orafetch $S" fetches a single row
     * and returns 0 (rows remain) or 1403 (end-of-data). */
    int                 returnRows      = 0;
    int                 asDict          = 0;

    uint32_t            numCols         = 0;
    Tcl_Size            numColsSize     = 0;
    OradpiFetchColMeta *meta            = NULL;
    OradpiFetchCell    *cells           = NULL;
    Tcl_Obj           **colNames        = NULL;
    Tcl_Obj           **colVals         = NULL;
    Tcl_Obj           **numberKeys      = NULL;
    Tcl_Obj            *rowsList        = NULL;
    Tcl_Obj            *rowObj          = NULL;
    uint64_t            fetched         = 0;
    int                 code            = TCL_OK;
    int                 needNames       = 0;
    size_t              metaBytes       = 0;
    size_t              cellBytes       = 0;
    size_t              colNameBytes    = 0;
    size_t              colValBytes     = 0;

    /* Snapshot handles for reentrancy safety, initialized to NULL
     * so the single cleanup label can conditionally release them. */
    dpiStmt            *fetchStmt       = NULL;
    GlobalConnRec      *fetchShared     = NULL;
    Tcl_Obj            *stmtNameSnap    = NULL;
    int                 fetchInlineLobs = 0;
    int                 fetchDead       = 0;

    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
        return TCL_ERROR;
    }
    OradpiStmt *st = Oradpi_LookupStmt(ip, objv[1]);
    if (!st)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!st->stmt)
        return Oradpi_SetError(ip, (OradpiBase *)st, -1, "statement is not prepared or connection closed");
    if (Oradpi_StmtIsAsyncBusy(st))
        return Oradpi_SetError(ip, (OradpiBase *)st, -1, "statement is busy (async operation in progress)");

    static const char *const fetchOpts[] = {"-datavariable", "-dataarray", "-indexbyname", "-indexbynumber", "-command", "-max", "-resultvariable", "-returnrows", "-asdict", NULL};
    enum FetchOptIdx { FOPT_DATAVAR, FOPT_DATAARRAY, FOPT_BYNAME, FOPT_BYNUMBER, FOPT_COMMAND, FOPT_MAX, FOPT_RESULTVAR, FOPT_RETURNROWS, FOPT_ASDICT };

    for (Tcl_Size i = 2; i < objc; i++) {
        int optIdx;
        if (Tcl_GetIndexFromObj(ip, objv[i], fetchOpts, "option", 0, &optIdx) != TCL_OK)
            return TCL_ERROR;

        switch ((enum FetchOptIdx)optIdx) {
        case FOPT_DATAVAR:
            if (i + 1 >= objc) {
                Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                return TCL_ERROR;
            }
            dataVar    = objv[++i];
            returnRows = 0;
            break;
        case FOPT_DATAARRAY:
            if (i + 1 >= objc) {
                Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                return TCL_ERROR;
            }
            dataArray  = objv[++i];
            returnRows = 0;
            break;
        case FOPT_BYNAME:
            indexByName = 1;
            returnRows  = 0;
            break;
        case FOPT_BYNUMBER:
            indexByNumber = 1;
            returnRows    = 0;
            break;
        case FOPT_COMMAND:
            if (i + 1 >= objc) {
                Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                return TCL_ERROR;
            }
            cmd        = objv[++i];
            returnRows = 0;
            break;
        case FOPT_MAX:
            if (i + 1 >= objc) {
                Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                return TCL_ERROR;
            }
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &maxRows) != TCL_OK)
                return TCL_ERROR;
            break;
        case FOPT_RESULTVAR:
            if (i + 1 >= objc) {
                Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
                return TCL_ERROR;
            }
            resultVar  = objv[++i];
            returnRows = 0;
            break;
        case FOPT_RETURNROWS:
            returnRows = 1;
            break;
        case FOPT_ASDICT:
            asDict     = 1;
            returnRows = 0;
            break;
        }
    }

    if (maxRows < 0)
        return Oradpi_SetError(ip, (OradpiBase *)st, -1, "orafetch: -max must be >= 0");
    /* Default to single-row fetch only in plain status-code mode.
     * When -command, -resultvar, or -returnrows is active, fetch all rows
     * unless the caller explicitly provides -max.  Note: this means
     * "orafetch $S -command {…}" on a large result set will iterate all
     * rows — callers should use -max to limit if the table is large. */
    if (!returnRows && !cmd && !resultVar && maxRows == 0)
        maxRows = 1;

    /* Column count is cached after the first fetch — skip the ODPI call
     * and gate acquire/release on every subsequent invocation. */
    if (st->fetchCacheNumCols > 0) {
        numCols = st->fetchCacheNumCols;
    } else {
        CONN_GATE_ENTER(st->owner);
        if (dpiStmt_getNumQueryColumns(st->stmt, &numCols) != DPI_SUCCESS) {
            CONN_GATE_LEAVE(st->owner);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getNumQueryColumns");
        }
        CONN_GATE_LEAVE(st->owner);
    }

    if (numCols == 0) {
        Tcl_SetObjResult(ip, returnRows ? Tcl_NewListObj(0, NULL) : Tcl_NewIntObj(1403));
        return TCL_OK;
    }

    numColsSize = (Tcl_Size)numCols;
    needNames   = (asDict || (dataArray && indexByName));

    /* Metadata cache check.  On the first orafetch after a parse the cache
     * is empty (fetchCacheNumCols == 0); we snapshot column info from ODPI
     * once and store it.  On every subsequent call we skip all ODPI
     * round-trips, DString allocations (upper_copy), and Tcl_ObjPrintf
     * calls and just IncrRefCount the already-built objects instead. */
    if (st->fetchCacheNumCols != numCols) {
        Oradpi_FreeFetchCache(st);

        if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(*meta), &metaBytes, "column metadata snapshot") != TCL_OK) {
            code = TCL_ERROR;
            goto cleanup;
        }
        meta = (OradpiFetchColMeta *)Tcl_Alloc(metaBytes);
        memset(meta, 0, metaBytes);
        if (SnapshotColumnMeta(ip, st, numCols, meta) != TCL_OK) {
            code = TCL_ERROR;
            goto cleanup;
        }

        st->fetchIsChar     = (int *)Tcl_Alloc(numCols * sizeof(int));
        st->fetchColNames   = (Tcl_Obj **)Tcl_Alloc(numCols * sizeof(Tcl_Obj *));
        st->fetchNumberKeys = (Tcl_Obj **)Tcl_Alloc(numCols * sizeof(Tcl_Obj *));
        memset(st->fetchColNames, 0, numCols * sizeof(Tcl_Obj *));
        memset(st->fetchNumberKeys, 0, numCols * sizeof(Tcl_Obj *));

        for (uint32_t c = 0; c < numCols; c++) {
            st->fetchIsChar[c]   = meta[c].isChar;
            st->fetchColNames[c] = meta[c].nameLen ? upper_copy(meta[c].name, meta[c].nameLen) : Tcl_NewStringObj("", 0);
            Tcl_IncrRefCount(st->fetchColNames[c]);
            st->fetchNumberKeys[c] = Tcl_ObjPrintf("%u", c);
            Tcl_IncrRefCount(st->fetchNumberKeys[c]);
        }
        st->fetchCacheNumCols = numCols;

        /* Build per-column output variable cache to eliminate N
         * dpiStmt_getQueryValue calls per row.  Done before FreeFetchMeta so
         * meta[c] type fields are still valid.  Object columns need a
         * dpiObjectType* unavailable here; any failure falls back to
         * dpiStmt_getQueryValue for the entire statement. */
        st->fetchVars         = (dpiVar **)Tcl_Alloc(numCols * sizeof(dpiVar *));
        st->fetchVarData      = (dpiData **)Tcl_Alloc(numCols * sizeof(dpiData *));
        st->fetchNativeTypes  = (dpiNativeTypeNum *)Tcl_Alloc(numCols * sizeof(dpiNativeTypeNum));
        memset(st->fetchVars, 0, numCols * sizeof(dpiVar *));
        memset(st->fetchVarData, 0, numCols * sizeof(dpiData *));

        int varBuildOk = 1;
        CONN_GATE_ENTER(st->owner);
        for (uint32_t c = 0; c < numCols; c++) {
            dpiVar  *var  = NULL;
            dpiData *data = NULL;
            if (meta[c].oracleTypeNum == DPI_ORACLE_TYPE_OBJECT) {
                varBuildOk = 0;
                break;
            }
            /* clientSizeInBytes is measured in bytes; pass sizeIsBytes=1 for
             * variable-length char/raw types.  For fixed-size types (NUMBER,
             * DATE, LOB, etc.) size is ignored by ODPI-C so both 0 and 1 are
             * safe — use 0 to be explicit. */
            int sizeIsBytes = (meta[c].clientSizeInBytes > 0) ? 1 : 0;
            if (dpiConn_newVar(st->owner->conn, meta[c].oracleTypeNum, meta[c].defaultNativeTypeNum, st->fetchArray, meta[c].clientSizeInBytes, sizeIsBytes, 0, NULL, &var, &data) != DPI_SUCCESS) {
                varBuildOk = 0;
                break;
            }
            if (dpiStmt_define(st->stmt, c + 1, var) != DPI_SUCCESS) {
                dpiVar_release(var);
                varBuildOk = 0;
                break;
            }
            st->fetchVars[c]        = var;
            st->fetchVarData[c]     = data;
            st->fetchNativeTypes[c] = meta[c].defaultNativeTypeNum;
        }
        CONN_GATE_LEAVE(st->owner);

        if (!varBuildOk) {
            for (uint32_t c = 0; c < numCols; c++)
                if (st->fetchVars[c])
                    dpiVar_release(st->fetchVars[c]);
            Tcl_Free((char *)st->fetchVars);
            st->fetchVars = NULL;
            Tcl_Free((char *)st->fetchVarData);
            st->fetchVarData = NULL;
            Tcl_Free((char *)st->fetchNativeTypes);
            st->fetchNativeTypes = NULL;
        }

        /* Compute LOB flag from the isChar/oracle-type metadata.
         * Checked once here; the fast path uses it to decide whether the
         * connection gate is needed per row. */
        int hasLob = 0;
        for (uint32_t c = 0; c < numCols && !hasLob; c++)
            if (meta[c].defaultNativeTypeNum == DPI_NATIVE_TYPE_LOB)
                hasLob = 1;
        st->fetchHasLobCols = hasLob;

        /* meta name copies served only to build the cache; free them now. */
        FreeFetchMeta(meta, numColsSize);
        meta = NULL;
    }

    if (needNames) {
        if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj *), &colNameBytes, "column name array") != TCL_OK) {
            code = TCL_ERROR;
            goto cleanup;
        }
        colNames = (Tcl_Obj **)Tcl_Alloc(colNameBytes);
        for (uint32_t c = 0; c < numCols; c++) {
            colNames[c] = st->fetchColNames[c];
            Tcl_IncrRefCount(colNames[c]);
        }
    }

    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj *), &colValBytes, "fetched row value array") != TCL_OK) {
        code = TCL_ERROR;
        goto cleanup;
    }
    colVals = (Tcl_Obj **)Tcl_Alloc(colValBytes);
    memset(colVals, 0, colValBytes);

    if (dataArray && indexByNumber) {
        size_t keyBytes = 0;
        if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(Tcl_Obj *), &keyBytes, "numeric column keys") != TCL_OK) {
            code = TCL_ERROR;
            goto cleanup;
        }
        numberKeys = (Tcl_Obj **)Tcl_Alloc(keyBytes);
        for (uint32_t c = 0; c < numCols; c++) {
            numberKeys[c] = st->fetchNumberKeys[c];
            Tcl_IncrRefCount(numberKeys[c]);
        }
    }

    if (returnRows || resultVar) {
        rowsList = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(rowsList);
    }

    if (Oradpi_CheckedAllocBytes(ip, numColsSize, sizeof(*cells), &cellBytes, "fetched row snapshot array") != TCL_OK) {
        code = TCL_ERROR;
        goto cleanup;
    }
    cells = (OradpiFetchCell *)Tcl_Alloc(cellBytes);
    memset(cells, 0, cellBytes);

    /* Snapshot dpiStmt* and GlobalConnRec* with addRef to hold
     * independent lifetime references during the fetch loop.  Callbacks
     * (-command) and variable traces (-datavariable, -dataarray,
     * -resultvariable) are reentrancy points where Tcl can legally call
     * oraclose or oralogoff, freeing the statement and connection wrappers
     * while the C function is still using them.
     *
     * With these holds, the underlying ODPI resources survive the loop
     * even if the Tcl-level wrappers are destroyed.  After each reentrancy
     * point, we verify liveness by checking if the statement is still
     * registered in the interp; if not, we break cleanly. */
    fetchStmt       = st->stmt;
    fetchShared     = st->owner ? st->owner->shared : NULL;
    fetchInlineLobs = st->owner ? st->owner->inlineLobs : 0;
    stmtNameSnap    = st->base.name;

    if (dpiStmt_addRef(fetchStmt) != DPI_SUCCESS) {
        code        = Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_addRef (fetch hold)");
        fetchStmt   = NULL; /* prevent cleanup from releasing a ref we never took */
        fetchShared = NULL;
        goto cleanup;
    }
    if (fetchShared)
        Oradpi_SharedConnAddRef(fetchShared);
    Tcl_IncrRefCount(stmtNameSnap);

    /* =========================================================================
     * Fetch loop — two paths share the same reentrancy zone:
     *
     *   Fast path (when st->fetchVarData != NULL):
     *     dpiStmt_fetchRows drains the ODPI buffer in one call per batch;
     *     per-row values come from direct dpiData[] buffer access — zero
     *     dpiStmt_getQueryValue ODPI calls in the hot inner loop.
     *     The gate is acquired once per batch (not per row), reducing lock
     *     contention against async workers or adopted-connection threads.
     *
     *   Fallback (st->fetchVarData == NULL):
     *     Original dpiStmt_fetch + dpiStmt_getQueryValue per row.
     *     Used when var creation failed (e.g. object-type columns).
     * ========================================================================= */
    if (st->fetchVarData) {
        /* ------------------------------------------------------------------
         * FAST PATH
         * ------------------------------------------------------------------ */
        int               moreRows   = 1;
        uint32_t          batchStart = 0, batchCount = 0, batchPos = 0;
        /* Snapshot cache pointers before reentrancy so callbacks that change
         * fetch settings (orastmt -fetchrows) cannot cause UAF — fetchDead
         * is checked at the top of every iteration. */
        dpiData         **varData     = st->fetchVarData;
        dpiNativeTypeNum *nativeTypes = st->fetchNativeTypes;

        for (;;) {
            if (fetchDead) {
                Tcl_SetObjResult(ip, Tcl_NewStringObj("orafetch: statement closed during callback", -1));
                code = TCL_ERROR;
                goto cleanup;
            }

            /* Drain the next batch when the current one is exhausted.
             * Pass maxRows (when limited) rather than fetchArray so ODPI-C
             * advances bufferRowIndex by at most the number of rows this call
             * will consume.  Without this, a fetchArray-sized advance discards
             * unprocessed buffered rows when the caller uses -max N < fetchArray
             * across multiple orafetch calls. */
            uint32_t batchLimit = (maxRows > 0 && maxRows < (Tcl_WideInt)st->fetchArray) ? (uint32_t)maxRows : st->fetchArray;
            if (batchPos >= batchCount) {
                if (!moreRows)
                    break;

                /* The gate is held only for the duration of one batch fetch
                 * (which may trigger a network round-trip), then released
                 * before processing any rows.  This allows async workers and
                 * adopted-connection threads to acquire the gate between
                 * batches. */
                Oradpi_SharedConnGateEnter(fetchShared);
                if (dpiStmt_fetchRows(fetchStmt, batchLimit, &batchStart, &batchCount, &moreRows) != DPI_SUCCESS) {
                    Oradpi_SharedConnGateLeave(fetchShared);
                    code = Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_fetchRows");
                    goto cleanup;
                }
                Oradpi_SharedConnGateLeave(fetchShared);

                if (batchCount == 0)
                    break;
                batchPos = 0;
            }

            uint32_t    rowIdx        = batchStart + batchPos++;
            const char *snapshotWhere = NULL;
            const char *snapshotMsg   = NULL;

            memset(colVals, 0, colValBytes);
            FreeFetchCells(cells, numColsSize, fetchShared);
            memset(cells, 0, cellBytes);

            /* Snapshot directly from pre-defined var buffers without
             * dpiStmt_getQueryValue calls.  For scalar-only queries
             * (fetchHasLobCols == 0), SnapshotCellLocked does only pure
             * struct copies with no ODPI calls, so the connection gate is
             * unnecessary.  Skipping it eliminates per-row lock contention
             * when multiple interpreters share one session.  For LOB queries
             * the gate is still needed for dpiLob_addRef. */
            int needGate = st->fetchHasLobCols;
            if (needGate)
                Oradpi_SharedConnGateEnter(fetchShared);
            for (uint32_t c = 0; c < numCols; c++) {
                dpiData *d = &varData[c][rowIdx];
                if (SnapshotCellLocked(fetchInlineLobs, nativeTypes[c], d, st->fetchIsChar[c], &cells[c], &snapshotWhere, &snapshotMsg) != TCL_OK) {
                    if (needGate)
                        Oradpi_SharedConnGateLeave(fetchShared);
                    if (snapshotWhere)
                        code = Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, snapshotWhere);
                    else {
                        Tcl_SetObjResult(ip, Tcl_NewStringObj(snapshotMsg ? snapshotMsg : "failed to snapshot fetched value", -1));
                        code = TCL_ERROR;
                    }
                    goto cleanup;
                }
            }
            if (needGate)
                Oradpi_SharedConnGateLeave(fetchShared);

            /* --- Reentrancy zone --- */
            for (uint32_t c = 0; c < numCols; c++) {
                colVals[c] = SnapshotCellToObj(ip, fetchShared, &cells[c]);
                if (!colVals[c]) {
                    code = TCL_ERROR;
                    goto cleanup;
                }
            }

            if (dataVar || rowsList) {
                rowObj = Tcl_NewListObj(0, NULL);
                Tcl_IncrRefCount(rowObj);
                for (uint32_t c = 0; c < numCols; c++) {
                    if (asDict) {
                        LAPPEND_GOTO(ip, rowObj, colNames[c], code, cleanup);
                        LAPPEND_GOTO(ip, rowObj, colVals[c], code, cleanup);
                    } else {
                        LAPPEND_GOTO(ip, rowObj, colVals[c], code, cleanup);
                    }
                }
            }

            if (dataArray) {
                if (indexByNumber) {
                    for (uint32_t c = 0; c < numCols; c++) {
                        if (!Tcl_ObjSetVar2(ip, dataArray, numberKeys[c], colVals[c], TCL_LEAVE_ERR_MSG)) {
                            code = TCL_ERROR;
                            goto cleanup;
                        }
                    }
                    if (!Oradpi_LookupStmt(ip, stmtNameSnap))
                        fetchDead = 1;
                } else if (indexByName) {
                    for (uint32_t c = 0; c < numCols; c++) {
                        if (!Tcl_ObjSetVar2(ip, dataArray, colNames[c], colVals[c], TCL_LEAVE_ERR_MSG)) {
                            code = TCL_ERROR;
                            goto cleanup;
                        }
                    }
                    if (!Oradpi_LookupStmt(ip, stmtNameSnap))
                        fetchDead = 1;
                }
            }

            if (!fetchDead && dataVar && !Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG)) {
                code = TCL_ERROR;
                goto cleanup;
            }
            if (!fetchDead && dataVar && !Oradpi_LookupStmt(ip, stmtNameSnap))
                fetchDead = 1;

            if (!fetchDead && cmd) {
                int evalCode = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
                if (!Oradpi_LookupStmt(ip, stmtNameSnap))
                    fetchDead = 1;
                if (evalCode == TCL_BREAK) {
                    if (rowObj) {
                        Tcl_DecrRefCount(rowObj);
                        rowObj = NULL;
                    }
                    memset(colVals, 0, colValBytes);
                    fetched++;
                    break;
                }
                if (evalCode == TCL_CONTINUE) {
                    if (rowObj) {
                        Tcl_DecrRefCount(rowObj);
                        rowObj = NULL;
                    }
                    memset(colVals, 0, colValBytes);
                    fetched++;
                    if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
                        break;
                    continue;
                }
                if (evalCode != TCL_OK) {
                    code = evalCode;
                    goto cleanup;
                }
            }

            if (rowsList) {
                LAPPEND_GOTO(ip, rowsList, rowObj, code, cleanup);
            }
            if (rowObj) {
                Tcl_DecrRefCount(rowObj);
                rowObj = NULL;
            }
            /* rowObj may have owned colVals[] elements; zero before next row. */
            memset(colVals, 0, colValBytes);
            fetched++;
            if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
                break;
        }
    } else {
        /* ------------------------------------------------------------------
         * FALLBACK PATH: dpiStmt_fetch + dpiStmt_getQueryValue per row
         * ------------------------------------------------------------------ */
        for (;;) {
            int         hasRow         = 0;
            uint32_t    bufferRowIndex = 0;
            const char *snapshotWhere  = NULL;
            const char *snapshotMsg    = NULL;

            if (fetchDead) {
                Tcl_SetObjResult(ip, Tcl_NewStringObj("orafetch: statement closed during callback", -1));
                code = TCL_ERROR;
                goto cleanup;
            }

            memset(colVals, 0, colValBytes);
            FreeFetchCells(cells, numColsSize, fetchShared);
            memset(cells, 0, cellBytes);

            Oradpi_SharedConnGateEnter(fetchShared);
            if (dpiStmt_fetch(fetchStmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS) {
                Oradpi_SharedConnGateLeave(fetchShared);
                code = Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_fetch");
                goto cleanup;
            }
            (void)bufferRowIndex;
            if (!hasRow) {
                Oradpi_SharedConnGateLeave(fetchShared);
                break;
            }

            for (uint32_t c = 1; c <= numCols; c++) {
                dpiNativeTypeNum nt;
                dpiData         *d = NULL;
                if (dpiStmt_getQueryValue(fetchStmt, c, &nt, &d) != DPI_SUCCESS) {
                    Oradpi_SharedConnGateLeave(fetchShared);
                    code = Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryValue");
                    goto cleanup;
                }
                if (SnapshotCellLocked(fetchInlineLobs, nt, d, st->fetchIsChar[c - 1], &cells[c - 1], &snapshotWhere, &snapshotMsg) != TCL_OK) {
                    Oradpi_SharedConnGateLeave(fetchShared);
                    if (snapshotWhere)
                        code = Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, snapshotWhere);
                    else {
                        Tcl_SetObjResult(ip, Tcl_NewStringObj(snapshotMsg ? snapshotMsg : "failed to snapshot fetched value", -1));
                        code = TCL_ERROR;
                    }
                    goto cleanup;
                }
            }
            Oradpi_SharedConnGateLeave(fetchShared);

            /* --- Reentrancy zone --- */
            for (uint32_t c = 0; c < numCols; c++) {
                colVals[c] = SnapshotCellToObj(ip, fetchShared, &cells[c]);
                if (!colVals[c]) {
                    code = TCL_ERROR;
                    goto cleanup;
                }
            }

            if (dataVar || rowsList) {
                rowObj = Tcl_NewListObj(0, NULL);
                Tcl_IncrRefCount(rowObj);
                for (uint32_t c = 0; c < numCols; c++) {
                    if (asDict) {
                        LAPPEND_GOTO(ip, rowObj, colNames[c], code, cleanup);
                        LAPPEND_GOTO(ip, rowObj, colVals[c], code, cleanup);
                    } else {
                        LAPPEND_GOTO(ip, rowObj, colVals[c], code, cleanup);
                    }
                }
            }

            if (dataArray) {
                if (indexByNumber) {
                    for (uint32_t c = 0; c < numCols; c++) {
                        if (!Tcl_ObjSetVar2(ip, dataArray, numberKeys[c], colVals[c], TCL_LEAVE_ERR_MSG)) {
                            code = TCL_ERROR;
                            goto cleanup;
                        }
                    }
                    if (!Oradpi_LookupStmt(ip, stmtNameSnap))
                        fetchDead = 1;
                } else if (indexByName) {
                    for (uint32_t c = 0; c < numCols; c++) {
                        if (!Tcl_ObjSetVar2(ip, dataArray, colNames[c], colVals[c], TCL_LEAVE_ERR_MSG)) {
                            code = TCL_ERROR;
                            goto cleanup;
                        }
                    }
                    if (!Oradpi_LookupStmt(ip, stmtNameSnap))
                        fetchDead = 1;
                }
            }

            if (!fetchDead && dataVar && !Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG)) {
                code = TCL_ERROR;
                goto cleanup;
            }
            if (!fetchDead && dataVar && !Oradpi_LookupStmt(ip, stmtNameSnap))
                fetchDead = 1;

            if (!fetchDead && cmd) {
                int evalCode = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
                if (!Oradpi_LookupStmt(ip, stmtNameSnap))
                    fetchDead = 1;
                if (evalCode == TCL_BREAK) {
                    if (rowObj) {
                        Tcl_DecrRefCount(rowObj);
                        rowObj = NULL;
                    }
                    memset(colVals, 0, colValBytes);
                    fetched++;
                    break;
                }
                if (evalCode == TCL_CONTINUE) {
                    if (rowObj) {
                        Tcl_DecrRefCount(rowObj);
                        rowObj = NULL;
                    }
                    memset(colVals, 0, colValBytes);
                    fetched++;
                    if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
                        break;
                    continue;
                }
                if (evalCode != TCL_OK) {
                    code = evalCode;
                    goto cleanup;
                }
            }

            if (rowsList) {
                LAPPEND_GOTO(ip, rowsList, rowObj, code, cleanup);
            }
            if (rowObj) {
                Tcl_DecrRefCount(rowObj);
                rowObj = NULL;
            }
            /* rowObj may have owned colVals[] elements; zero before next row. */
            memset(colVals, 0, colValBytes);
            fetched++;
            if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
                break;
        }
    }

    if (resultVar) {
        Tcl_Obj *resultObj = rowsList ? rowsList : Tcl_NewListObj(0, NULL);
        if (!Tcl_ObjSetVar2(ip, resultVar, NULL, resultObj, TCL_LEAVE_ERR_MSG)) {
            code = TCL_ERROR;
            goto cleanup;
        }
    }

    if (returnRows) {
        Tcl_SetObjResult(ip, rowsList ? rowsList : Tcl_NewListObj(0, NULL));
        code = TCL_OK;
        goto cleanup;
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(fetched > 0 ? 0 : 1403));
    code = TCL_OK;

cleanup:
    /* Release the addRef'd snapshot handles if they were acquired.
     * Pre-loop gotos reach here with fetchStmt == NULL (never addRef'd). */
    if (cells) {
        FreeFetchCells(cells, numColsSize, fetchShared);
        Tcl_Free((char *)cells);
    }
    if (fetchStmt)
        dpiStmt_release(fetchStmt);
    if (fetchShared)
        Oradpi_SharedConnRelease(fetchShared);
    if (stmtNameSnap)
        Tcl_DecrRefCount(stmtNameSnap);
    if (rowObj)
        Tcl_DecrRefCount(rowObj);
    if (rowsList)
        Tcl_DecrRefCount(rowsList);
    if (numberKeys) {
        for (uint32_t c = 0; c < numCols; c++) {
            if (numberKeys[c])
                Tcl_DecrRefCount(numberKeys[c]);
        }
        Tcl_Free((char *)numberKeys);
    }
    if (colVals) {
        /* colVals[] entries are owned by rowObj or rowsList — they are
         * released when those containers are freed above.  Any surviving
         * non-NULL entry here still has a live owner; we must not touch
         * its refcount.  Just free the pointer array itself. */
        Tcl_Free((char *)colVals);
    }
    if (colNames) {
        for (uint32_t c = 0; c < numCols; c++) {
            if (colNames[c])
                Tcl_DecrRefCount(colNames[c]);
        }
        Tcl_Free((char *)colNames);
    }
    if (meta)
        FreeFetchMeta(meta, numColsSize);
    return code;
}
