/*
 *  cmd_fetch.c --
 *
 *    Row fetch and result materialization.
 *
 *        - Maps ODPI column types to Tcl objects; supports `-max`, name/position addressing, LOB streaming,
 *          and optional per‑row callbacks.
 *        - Per‑interp data paths only; no process‑global state; thread‑safe access to result buffers.
 *
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cmd_int.h"
#include "dpi.h"
#include "state.h"

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

static Tcl_Obj *upper_copy(const char *s, uint32_t n) {
    Tcl_Obj *o = Tcl_NewStringObj(s, (Tcl_Size)n);
    char    *p = Tcl_GetString(o);
    for (uint32_t i = 0; i < n; i++)
        if (p[i] >= 'a' && p[i] <= 'z')
            p[i] = (char)(p[i] - 'a' + 'A');
    return o;
}

static Tcl_Obj *ValueToObj(Tcl_Interp *ip, OradpiStmt *st, dpiNativeTypeNum nt, dpiData *d, int colIsChar) {
    if (!d || d->isNull)
        return Tcl_NewObj();

    switch (nt) {
    case DPI_NATIVE_TYPE_INT64: {
        long long v = d->value.asInt64;
        if (v >= INT_MIN && v <= INT_MAX)
            return Tcl_NewIntObj((int)v);
        return Tcl_NewWideIntObj((Tcl_WideInt)v);
    }
    case DPI_NATIVE_TYPE_UINT64:
        return Tcl_NewWideIntObj((Tcl_WideInt)d->value.asUint64);
    case DPI_NATIVE_TYPE_FLOAT: {
        double dv = (double)d->value.asFloat;
        if (isfinite(dv)) {
            double ip;
            if (modf(dv, &ip) == 0.0) {
                if (ip >= (double)INT_MIN && ip <= (double)INT_MAX)
                    return Tcl_NewIntObj((int)ip);
                if (ip >= (double)LLONG_MIN && ip <= (double)LLONG_MAX)
                    return Tcl_NewWideIntObj((Tcl_WideInt)((long long)ip));
            }
        }
        return Tcl_NewDoubleObj(dv);
    }
    case DPI_NATIVE_TYPE_DOUBLE: {
        double dv = (double)d->value.asDouble;
        if (isfinite(dv)) {
            double ip;
            if (modf(dv, &ip) == 0.0) {
                if (ip >= (double)INT_MIN && ip <= (double)INT_MAX)
                    return Tcl_NewIntObj((int)ip);
                if (ip >= (double)LLONG_MIN && ip <= (double)LLONG_MAX)
                    return Tcl_NewWideIntObj((Tcl_WideInt)((long long)ip));
            }
        }
        return Tcl_NewDoubleObj(dv);
    }
    case DPI_NATIVE_TYPE_BOOLEAN:
        return Tcl_NewBooleanObj(d->value.asBoolean ? 1 : 0);
    case DPI_NATIVE_TYPE_TIMESTAMP: {
        const dpiTimestamp *ts = &d->value.asTimestamp;
        char                buf[64];
        int                 n = snprintf(buf, sizeof(buf), "%04d-%02u-%02uT%02u:%02u:%02u.%06u", ts->year, ts->month, ts->day, ts->hour, ts->minute, ts->second, ts->fsecond / 1000);
        return Tcl_NewStringObj(buf, (Tcl_Size)((n < 0) ? 0 : n));
    }
    case DPI_NATIVE_TYPE_BYTES: {
        const dpiBytes *b = &d->value.asBytes;
        if (!b->ptr)
            return Tcl_NewObj();
        if (colIsChar) {
            return Tcl_NewStringObj(b->ptr, (Tcl_Size)b->length);
        } else {
            return Tcl_NewByteArrayObj((const unsigned char *)b->ptr, (Tcl_Size)b->length);
        }
    }
    case DPI_NATIVE_TYPE_LOB: {
        OradpiConn *co  = st->owner;
        dpiLob     *lob = d->value.asLOB;
        if (!lob)
            return Tcl_NewObj();

        if (co && co->inlineLobs == 0) {
            dpiLob_addRef(lob);
            OradpiLob *L = Oradpi_NewLob(ip, lob);
            return L->base.name;
        }

        uint64_t sizeCharsOrBytes = 0;
        if (dpiLob_getSize(lob, &sizeCharsOrBytes) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiLob_getSize"), NULL;
        if (sizeCharsOrBytes == 0)
            return Tcl_NewObj();

        uint64_t capBytes = 0;
        if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiLob_getBufferSize"), NULL;

        const uint64_t MAX_INLINE = (1u << 20);
        if (capBytes > MAX_INLINE && capBytes > 0) {
            uint64_t scaledChars = (sizeCharsOrBytes * MAX_INLINE) / capBytes;
            if (scaledChars == 0)
                scaledChars = 1;
            sizeCharsOrBytes = scaledChars;
            if (dpiLob_getBufferSize(lob, sizeCharsOrBytes, &capBytes) != DPI_SUCCESS)
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiLob_getBufferSize"), NULL;
        }

        char    *buf      = (char *)Tcl_Alloc((size_t)capBytes);
        uint64_t gotBytes = capBytes;
        if (dpiLob_readBytes(lob, 1, sizeCharsOrBytes, buf, &gotBytes) != DPI_SUCCESS) {
            Tcl_Free(buf);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiLob_readBytes"), NULL;
        }

        Tcl_Obj *out = colIsChar ? Tcl_NewStringObj(buf, (Tcl_Size)gotBytes) : Tcl_NewByteArrayObj((unsigned char *)buf, (Tcl_Size)gotBytes);
        Tcl_Free(buf);
        return out;
    }
    default:
        return Tcl_NewObj();
    }
}

int Oradpi_Cmd_Fetch(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?options?");
        return TCL_ERROR;
    }
    OradpiStmt *st = Oradpi_LookupStmt(ip, objv[1]);
    if (!st)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    Tcl_Obj    *dataVar     = NULL;
    Tcl_Obj    *dataArray   = NULL;
    int         indexByName = 0, indexByNumber = 0;
    Tcl_Obj    *cmd        = NULL;
    Tcl_WideInt maxRows    = 0;
    Tcl_Obj    *resultVar  = NULL;
    int         returnRows = 0;
    int         asDict     = 0;

    for (Tcl_Size i = 2; i < objc; i++) {
        const char *o = Tcl_GetString(objv[i]);
        if (strcmp(o, "-datavariable") == 0 && i + 1 < objc)
            dataVar = objv[++i];
        else if (strcmp(o, "-dataarray") == 0 && i + 1 < objc)
            dataArray = objv[++i];
        else if (strcmp(o, "-indexbyname") == 0)
            indexByName = 1;
        else if (strcmp(o, "-indexbynumber") == 0)
            indexByNumber = 1;
        else if (strcmp(o, "-command") == 0 && i + 1 < objc)
            cmd = objv[++i];
        else if (strcmp(o, "-max") == 0 && i + 1 < objc) {
            if (Tcl_GetWideIntFromObj(ip, objv[++i], &maxRows) != TCL_OK)
                return TCL_ERROR;
        } else if (strcmp(o, "-resultvariable") == 0 && i + 1 < objc)
            resultVar = objv[++i];
        else if (strcmp(o, "-returnrows") == 0)
            returnRows = 1;
        else if (strcmp(o, "-asdict") == 0)
            asDict = 1;
        else
            returnRows = 1;
    }

    if (!returnRows && maxRows <= 0) {
        maxRows = 1;
    }

    uint32_t numCols = 0;
    if (dpiStmt_getNumQueryColumns(st->stmt, &numCols) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getNumQueryColumns");

    Tcl_Obj **colNames  = NULL;
    int      *colIsChar = (int *)Tcl_Alloc(numCols * sizeof(int));
    for (uint32_t c = 1; c <= numCols; c++) {
        dpiQueryInfo qi;
        if (dpiStmt_getQueryInfo(st->stmt, c, &qi) != DPI_SUCCESS) {
            Tcl_Free(colIsChar);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryInfo");
        }
        colIsChar[c - 1] = is_char_type(qi.typeInfo.oracleTypeNum);
    }
    if (asDict || (dataArray && indexByName)) {
        colNames = (Tcl_Obj **)Tcl_Alloc(numCols * sizeof(Tcl_Obj *));
        for (uint32_t c = 1; c <= numCols; c++) {
            dpiQueryInfo qi;
            if (dpiStmt_getQueryInfo(st->stmt, c, &qi) != DPI_SUCCESS) {
                Tcl_Free(colIsChar);
                Tcl_Free(colNames);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryInfo");
            }
            colNames[c - 1] = upper_copy(qi.name, qi.nameLength);
            Tcl_IncrRefCount(colNames[c - 1]);
        }
    }

    if (!returnRows && maxRows == 1) {
        int      hasRow         = 0;
        uint32_t bufferRowIndex = 0;
        if (dpiStmt_fetch(st->stmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS) {
            if (colNames) {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free(colNames);
            }
            Tcl_Free(colIsChar);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_fetch");
        }
        if (!hasRow) {
            if (colNames) {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free(colNames);
            }
            Tcl_Free(colIsChar);
            Tcl_SetObjResult(ip, Tcl_NewIntObj(1403));
            return TCL_OK;
        }
        Tcl_Obj *rowObj = Tcl_NewListObj(0, NULL);
        for (uint32_t c = 1; c <= numCols; c++) {
            dpiNativeTypeNum nt;
            dpiData         *d = NULL;
            if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryValue");
            }
            Tcl_Obj *v = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
            if (!v) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                return TCL_ERROR;
            }
            if (asDict) {
                Tcl_ListObjAppendElement(ip, rowObj, colNames[c - 1]);
                Tcl_ListObjAppendElement(ip, rowObj, v);
            } else {
                Tcl_ListObjAppendElement(ip, rowObj, v);
            }
        }
        if (dataVar && !Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG)) {
            if (colNames) {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free(colNames);
            }
            Tcl_Free(colIsChar);
            return TCL_ERROR;
        }
        if (cmd) {
            Tcl_IncrRefCount(rowObj);
            int code = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
            Tcl_DecrRefCount(rowObj);
            if (code != TCL_OK) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                return code;
            }
        }
        if (colNames) {
            for (uint32_t i = 0; i < numCols; i++)
                Tcl_DecrRefCount(colNames[i]);
            Tcl_Free(colNames);
        }
        Tcl_Free(colIsChar);
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    Tcl_Obj *rowsList = NULL;
    if (returnRows) {
        rowsList = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(rowsList);
    }

    uint64_t fetched = 0;
    for (;;) {
        int      hasRow         = 0;
        uint32_t bufferRowIndex = 0;
        if (dpiStmt_fetch(st->stmt, &hasRow, &bufferRowIndex) != DPI_SUCCESS) {
            if (colNames) {
                for (uint32_t i = 0; i < numCols; i++)
                    Tcl_DecrRefCount(colNames[i]);
                Tcl_Free(colNames);
            }
            Tcl_Free(colIsChar);
            if (rowsList)
                Tcl_DecrRefCount(rowsList);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_fetch");
        }
        if (!hasRow)
            break;

        Tcl_Obj *rowObj = Tcl_NewListObj(0, NULL);
        for (uint32_t c = 1; c <= numCols; c++) {
            dpiNativeTypeNum nt;
            dpiData         *d = NULL;
            if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryValue");
            }
            Tcl_Obj *v = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
            if (!v) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return TCL_ERROR;
            }
            if (asDict) {
                Tcl_ListObjAppendElement(ip, rowObj, colNames[c - 1]);
                Tcl_ListObjAppendElement(ip, rowObj, v);
            } else {
                Tcl_ListObjAppendElement(ip, rowObj, v);
            }
        }

        if (dataArray) {
            if (indexByNumber) {
                for (uint32_t c = 1; c <= numCols; c++) {
                    dpiNativeTypeNum nt;
                    dpiData         *d = NULL;
                    if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS) {
                        if (colNames) {
                            for (uint32_t i = 0; i < numCols; i++)
                                Tcl_DecrRefCount(colNames[i]);
                            Tcl_Free(colNames);
                        }
                        Tcl_Free(colIsChar);
                        if (rowsList)
                            Tcl_DecrRefCount(rowsList);
                        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryValue");
                    }
                    Tcl_Obj *v = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
                    char     key[16];
                    int      n = snprintf(key, sizeof(key), "%u", c);
                    if (!Tcl_ObjSetVar2(ip, dataArray, Tcl_NewStringObj(key, n), v, TCL_LEAVE_ERR_MSG)) {
                        if (colNames) {
                            for (uint32_t i = 0; i < numCols; i++)
                                Tcl_DecrRefCount(colNames[i]);
                            Tcl_Free(colNames);
                        }
                        Tcl_Free(colIsChar);
                        if (rowsList)
                            Tcl_DecrRefCount(rowsList);
                        return TCL_ERROR;
                    }
                }
            } else if (indexByName) {
                for (uint32_t c = 1; c <= numCols; c++) {
                    dpiNativeTypeNum nt;
                    dpiData         *d = NULL;
                    if (dpiStmt_getQueryValue(st->stmt, c, &nt, &d) != DPI_SUCCESS) {
                        if (colNames) {
                            for (uint32_t i = 0; i < numCols; i++)
                                Tcl_DecrRefCount(colNames[i]);
                            Tcl_Free(colNames);
                        }
                        Tcl_Free(colIsChar);
                        if (rowsList)
                            Tcl_DecrRefCount(rowsList);
                        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)st, "dpiStmt_getQueryValue");
                    }
                    Tcl_Obj *v = ValueToObj(ip, st, nt, d, colIsChar[c - 1]);
                    if (!Tcl_ObjSetVar2(ip, dataArray, colNames[c - 1], v, TCL_LEAVE_ERR_MSG)) {
                        if (colNames) {
                            for (uint32_t i = 0; i < numCols; i++)
                                Tcl_DecrRefCount(colNames[i]);
                            Tcl_Free(colNames);
                        }
                        Tcl_Free(colIsChar);
                        if (rowsList)
                            Tcl_DecrRefCount(rowsList);
                        return TCL_ERROR;
                    }
                }
            }
        }

        if (dataVar) {
            if (!Tcl_ObjSetVar2(ip, dataVar, NULL, rowObj, TCL_LEAVE_ERR_MSG)) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return TCL_ERROR;
            }
        }
        if (cmd) {
            Tcl_IncrRefCount(rowObj);
            int code = Tcl_EvalObjEx(ip, cmd, TCL_EVAL_GLOBAL);
            Tcl_DecrRefCount(rowObj);
            if (code != TCL_OK) {
                if (colNames) {
                    for (uint32_t i = 0; i < numCols; i++)
                        Tcl_DecrRefCount(colNames[i]);
                    Tcl_Free(colNames);
                }
                Tcl_Free(colIsChar);
                if (rowsList)
                    Tcl_DecrRefCount(rowsList);
                return code;
            }
        }
        if (rowsList) {
            Tcl_ListObjAppendElement(ip, rowsList, rowObj);
        }

        fetched++;
        if (maxRows > 0 && (Tcl_WideInt)fetched >= maxRows)
            break;
    }

    if (colNames) {
        for (uint32_t i = 0; i < numCols; i++)
            Tcl_DecrRefCount(colNames[i]);
        Tcl_Free(colNames);
    }
    Tcl_Free(colIsChar);

    if (resultVar) {
        Tcl_ObjSetVar2(ip, resultVar, NULL, rowsList ? rowsList : Tcl_NewListObj(0, NULL), TCL_LEAVE_ERR_MSG);
    }
    if (returnRows) {
        Tcl_SetObjResult(ip, rowsList ? rowsList : Tcl_NewListObj(0, NULL));
        return TCL_OK;
    }
    if (rowsList)
        Tcl_DecrRefCount(rowsList);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(fetched > 0 ? 0 : 1403));
    return TCL_OK;
}
