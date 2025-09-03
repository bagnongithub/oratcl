/*
 *  cmd_exec.c --
 *
 *
 *    SQL execution helpers and convenience commands (single-shot and prepared executes).
 *
 *    - Implements Oratcl-compatible autocommit and rows-affected tracking.
 *    - Designed for use from worker threads: ODPI calls are serialized per statement/connection as needed.
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

#ifdef _WIN32
#include <ctype.h>
// Case-insensitive version of strstr
char* strcasestr(const char* haystack, const char* needle) {
    if (!haystack || !needle) return NULL;

    size_t needle_len = strlen(needle);
    if (needle_len == 0) return (char*)haystack;

    for (; *haystack; haystack++) {
        // Compare substrings of length needle_len
        if (_strnicmp(haystack, needle, needle_len) == 0) {
            return (char*)haystack;
        }
    }

    return NULL;
}
#endif

void            Oradpi_PendingsForget(Tcl_Interp *, const char *);

extern uint32_t with_colon(const char *nameNoColon, char *buf, uint32_t bufsz);

typedef struct BindStore {
    Tcl_HashTable byName;
} BindStore;
typedef struct BindStoreMap {
    Tcl_HashTable byStmt;
} BindStoreMap;
#define BINDSTORE_ASSOC "oradpi.bindstore"

static BindStore *LookupBindStore(Tcl_Interp *ip, const char *stmtKey) {
    BindStoreMap *bm = (BindStoreMap *)Tcl_GetAssocData(ip, BINDSTORE_ASSOC, NULL);
    if (!bm)
        return NULL;
    Tcl_HashEntry *he = Tcl_FindHashEntry(&bm->byStmt, stmtKey);
    return he ? (BindStore *)Tcl_GetHashValue(he) : NULL;
}

static void ClearBindStoreForStmt(Tcl_Interp *ip, const char *stmtKey) {
    BindStoreMap *bm = (BindStoreMap *)Tcl_GetAssocData(ip, BINDSTORE_ASSOC, NULL);
    if (!bm)
        return;
    Tcl_HashEntry *he = Tcl_FindHashEntry(&bm->byStmt, stmtKey);
    if (!he)
        return;
    BindStore *bs = (BindStore *)Tcl_GetHashValue(he);
    if (bs) {
        Tcl_HashSearch hs;
        for (Tcl_HashEntry *e = Tcl_FirstHashEntry(&bs->byName, &hs); e; e = Tcl_NextHashEntry(&hs)) {
            Tcl_Obj *v = (Tcl_Obj *)Tcl_GetHashValue(e);
            if (v)
                Tcl_DecrRefCount(v);
        }
        Tcl_DeleteHashTable(&bs->byName);
        Tcl_Free((char *)bs);
    }
    Tcl_DeleteHashEntry(he);
}

typedef struct PendingRefs {
    int      n, cap;
    dpiVar **vars;
} PendingRefs;

static void Pendings_Init(PendingRefs *pr) {
    pr->n    = 0;
    pr->cap  = 8;
    pr->vars = (dpiVar **)Tcl_Alloc(sizeof(dpiVar *) * pr->cap);
}

static void Pendings_Add(PendingRefs *pr, dpiVar *v) {
    if (pr->n == pr->cap) {
        pr->cap *= 2;
        pr->vars = (dpiVar **)Tcl_Realloc((char *)pr->vars, sizeof(dpiVar *) * pr->cap);
    }
    pr->vars[pr->n++] = v;
}

static void Pendings_ReleaseAll(PendingRefs *pr) {
    for (int i = 0; i < pr->n; i++)
        if (pr->vars[i])
            dpiVar_release(pr->vars[i]);
    Tcl_Free((char *)pr->vars);
    pr->n = pr->cap = 0;
    pr->vars        = NULL;
}

static int BindValueByNameDual(OradpiStmt *s, const char *nameNoColon, dpiNativeTypeNum ntn, dpiData *d, Tcl_Interp *ip, const char *ctx) {
    uint32_t nlen = (uint32_t)strlen(nameNoColon);
    if (dpiStmt_bindValueByName(s->stmt, nameNoColon, nlen, ntn, d) == DPI_SUCCESS)
        return TCL_OK;
    char     buf[256];
    uint32_t m = with_colon(nameNoColon, buf, sizeof(buf));
    if (m && dpiStmt_bindValueByName(s->stmt, buf, m, ntn, d) == DPI_SUCCESS)
        return TCL_OK;
    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, ctx);
}

static int BindVarByNameDual(OradpiStmt *s, const char *nameNoColon, dpiVar *var, Tcl_Interp *ip, const char *ctx) {
    uint32_t nlen = (uint32_t)strlen(nameNoColon);
    if (dpiStmt_bindByName(s->stmt, nameNoColon, nlen, var) == DPI_SUCCESS)
        return TCL_OK;
    char     buf[256];
    uint32_t m = with_colon(nameNoColon, buf, sizeof(buf));
    if (m && dpiStmt_bindByName(s->stmt, buf, m, var) == DPI_SUCCESS)
        return TCL_OK;
    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, ctx);
}

static int BindOneLobScalar_Pending(Tcl_Interp *ip, OradpiStmt *s, PendingRefs *pr, const char *nameNoColon, dpiOracleTypeNum lobType, const char *buf, uint32_t buflen) {
    dpiVar  *var  = NULL;
    dpiData *data = NULL;
    if (dpiConn_newVar(s->owner->conn, lobType, DPI_NATIVE_TYPE_LOB, 1, 0, 0, 0, NULL, &var, &data) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_newVar(LOB)");

    dpiLob *lob = NULL;
    if (dpiConn_newTempLob(s->owner->conn, lobType, &lob) != DPI_SUCCESS) {
        dpiVar_release(var);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_newTempLob");
    }
    if (buflen > 0) {
        if (dpiLob_setFromBytes(lob, buf, (uint64_t)buflen) != DPI_SUCCESS) {
            dpiLob_release(lob);
            dpiVar_release(var);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiLob_setFromBytes");
        }
    }
    if (dpiVar_setFromLob(var, 0, lob) != DPI_SUCCESS) {
        dpiLob_release(lob);
        dpiVar_release(var);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiVar_setFromLob");
    }
    dpiLob_release(lob);

    if (BindVarByNameDual(s, nameNoColon, var, ip, "dpiStmt_bindByName(LOB)") != TCL_OK) {
        dpiVar_release(var);
        return TCL_ERROR;
    }

    Pendings_Add(pr, var);
    return TCL_OK;
}

static int BindOneByValue_Pending(Tcl_Interp *ip, OradpiStmt *s, PendingRefs *pr, const char *nameNoColon, Tcl_Obj *valueObj) {
    int forceBlob = 0;
    {
        const char *n = nameNoColon ? nameNoColon : "";
        size_t      L = strlen(n);
        if (L == 1 && (n[0] == 'b' || n[0] == 'B'))
            forceBlob = 1;
        if (!forceBlob) {
            const char *p = strcasestr(n, "blob");
            if (p)
                forceBlob = 1;
        }
    }
    if (forceBlob) {
        Tcl_Size             blen = 0;
        const unsigned char *bp   = (valueObj->typePtr == Tcl_GetObjType("bytearray")) ? Tcl_GetByteArrayFromObj(valueObj, &blen) : (const unsigned char *)Tcl_GetStringFromObj(valueObj, &blen);
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char *)bp, (uint32_t)blen);
    }

    if (valueObj->typePtr == Tcl_GetObjType("bytearray")) {
        Tcl_Size       blen = 0;
        unsigned char *b    = Tcl_GetByteArrayFromObj(valueObj, &blen);
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char *)b, (uint32_t)blen);
    }

    Tcl_Size    sl = 0;
    const char *sv = Tcl_GetStringFromObj(valueObj, &sl);

    if (sl > 0 && memchr(sv, '\0', (size_t)sl) != NULL)
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, (uint32_t)sl);

    int clobHint = 0;
    {
        const char *n = nameNoColon ? nameNoColon : "";
        size_t      L = strlen(n);
        if (L == 1 && (n[0] == 'c' || n[0] == 'C'))
            clobHint = 1;
        if (!clobHint && strcasestr(n, "clob"))
            clobHint = 1;
    }
    if (clobHint && sl > 0) {
        if (sl > 4000)
            return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, (uint32_t)sl);
    }

    if (sl > 4000)
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, (uint32_t)sl);

    dpiData d;
    memset(&d, 0, sizeof d);
    Tcl_WideInt wi;
    double      dd;
    if (Tcl_GetWideIntFromObj(NULL, valueObj, &wi) == TCL_OK) {
        d.value.asInt64 = (int64_t)wi;
        d.isNull        = 0;
        return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_INT64, &d, ip, "dpiStmt_bindValueByName(INT64)");
    }
    if (Tcl_GetDoubleFromObj(NULL, valueObj, &dd) == TCL_OK) {
        d.value.asDouble = dd;
        d.isNull         = 0;
        return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_DOUBLE, &d, ip, "dpiStmt_bindValueByName(DOUBLE)");
    }

    dpiEncodingInfo enc;
    memset(&enc, 0, sizeof enc);
    (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);
    d.value.asBytes.ptr      = (char *)sv;
    d.value.asBytes.length   = (uint32_t)sl;
    d.value.asBytes.encoding = enc.encoding;
    d.isNull                 = 0;
    return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_BYTES, &d, ip, "dpiStmt_bindValueByName(BYTES)");
}

static int RebindAllStored_Pending(Tcl_Interp *ip, OradpiStmt *s, PendingRefs *pr, BindStore *bs) {
    if (!bs)
        return TCL_OK;
    Tcl_HashSearch hs;
    for (Tcl_HashEntry *he = Tcl_FirstHashEntry(&bs->byName, &hs); he; he = Tcl_NextHashEntry(&hs)) {
        const char *nameNoColon = (const char *)Tcl_GetHashKey(&bs->byName, he);
        Tcl_Obj    *val         = (Tcl_Obj *)Tcl_GetHashValue(he);
        if (!val)
            continue;
        if (BindOneByValue_Pending(ip, s, pr, nameNoColon, val) != TCL_OK)
            return TCL_ERROR;
    }
    return TCL_OK;
}

static int ExecOnce_WithRebind(Tcl_Interp *ip, OradpiStmt *s, const char *skey, int doCommit) {
    PendingRefs pr;
    Pendings_Init(&pr);

    BindStore *bs = LookupBindStore(ip, skey);
    if (bs && RebindAllStored_Pending(ip, s, &pr, bs) != TCL_OK) {
        Pendings_ReleaseAll(&pr);
        return TCL_ERROR;
    }

    dpiStmtInfo info;
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
        Pendings_ReleaseAll(&pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getInfo");
    }

    dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
    if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
        mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

    uint32_t nqc = 0;
    if (dpiStmt_execute(s->stmt, mode, &nqc) != DPI_SUCCESS) {
        Pendings_ReleaseAll(&pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_execute");
    }

    uint64_t rows = 0;
    if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
        Oradpi_RecordRows((OradpiBase *)s, rows);
    Oradpi_UpdateStmtType(s);

    Pendings_ReleaseAll(&pr);
    Oradpi_PendingsForget(ip, skey);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Exec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2 || objc > 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int doCommit = 0;
    if (objc == 3) {
        const char *o = Tcl_GetString(objv[2]);
        if (strcmp(o, "-commit") == 0)
            doCommit = 1;
        else {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
            return TCL_ERROR;
        }
    }

    const char *skey = Tcl_GetString(objv[1]);
    return ExecOnce_WithRebind(ip, s, skey, doCommit);
}

int Oradpi_Cmd_StmtSql(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 3 || objc > 4) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle SQL ?-commit?");
        return TCL_ERROR;
    }
    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int doCommit = 0;
    if (objc == 4) {
        const char *o = Tcl_GetString(objv[3]);
        if (strcmp(o, "-commit") == 0)
            doCommit = 1;
        else {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle SQL ?-commit?");
            return TCL_ERROR;
        }
    }

    Tcl_Size    slen    = 0;
    const char *sql     = Tcl_GetStringFromObj(objv[2], &slen);
    dpiStmt    *newStmt = NULL;
    if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)slen, NULL, 0, &newStmt) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiConn_prepareStmt");

    if (s->stmt)
        dpiStmt_release(s->stmt);
    s->stmt          = newStmt;

    const char *skey = Tcl_GetString(objv[1]);
    ClearBindStoreForStmt(ip, skey);

    return ExecOnce_WithRebind(ip, s, skey, doCommit);
}

int Oradpi_Cmd_Plexec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    Tcl_Obj *blockObj = NULL;
    int      doCommit = 0;

    int      argi     = 2;
    while (argi < objc) {
        const char *t = Tcl_GetString(objv[argi]);
        if (strcmp(t, "-commit") == 0) {
            doCommit = 1;
            argi++;
            continue;
        }
        if (!blockObj) {
            blockObj = objv[argi++];
            continue;
        }
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?{PLSQL block}? ?-commit?");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    if (blockObj) {
        Tcl_Size    bl      = 0;
        const char *sql     = Tcl_GetStringFromObj(blockObj, &bl);
        dpiStmt    *newStmt = NULL;
        if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)bl, NULL, 0, &newStmt) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s->owner, "dpiConn_prepareStmt");
        if (s->stmt)
            dpiStmt_release(s->stmt);
        s->stmt = newStmt;
        ClearBindStoreForStmt(ip, Tcl_GetString(objv[1]));
    }

    const char *skey = Tcl_GetString(objv[1]);
    return ExecOnce_WithRebind(ip, s, skey, doCommit);
}
