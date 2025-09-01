/*
 *  cmd_bind.c --
 *
 *    Bind variable creation and management for statements.
 *
 *        - Uses ODPI‑C typed variables and buffers; supports array DML and name/position binds.
 *        - Per‑statement caches live inside the current interpreter; no global mutable state; mutexes protect
 *          any transient shared tables used for performance.
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

uint32_t with_colon(const char *nameNoColon, char *dst, uint32_t cap) {
    if (!nameNoColon)
        nameNoColon = "";
    size_t n = strlen(nameNoColon);
    if (cap < (uint32_t)(n + 2))
        return 0;
    dst[0] = ':';
    memcpy(dst + 1, nameNoColon, n);
    dst[n + 1] = '\0';
    return (uint32_t)(n + 1);
}

static const char *strip_colon(const char *raw, uint32_t *nlenOut) {
    const char *p = raw ? raw : "";
    if (*p == ':')
        p++;
    if (nlenOut)
        *nlenOut = (uint32_t)strlen(p);
    return p;
}

static int strcasestr_contains(const char *hay, const char *needle) {
    if (!hay || !needle || !*needle)
        return 0;
    size_t nl = strlen(needle);
    for (const char *p = hay; *p; ++p) {
#ifdef _WIN32
        if (_strnicmp(p, needle, nl) == 0)
            return 1;
#else
        if (strncasecmp(p, needle, nl) == 0)
            return 1;
#endif
    }
    return 0;
}

static int is_blob_hint(const char *nameNoColon) {
    if (!nameNoColon || !*nameNoColon)
        return 0;
    if ((nameNoColon[0] == 'b' || nameNoColon[0] == 'B') && nameNoColon[1] == '\0')
        return 1;
    return strcasestr_contains(nameNoColon, "blob");
}
static int is_clob_hint(const char *nameNoColon) {
    if (!nameNoColon || !*nameNoColon)
        return 0;
    if ((nameNoColon[0] == 'c' || nameNoColon[0] == 'C') && nameNoColon[1] == '\0')
        return 1;
    return strcasestr_contains(nameNoColon, "clob");
}

typedef struct BindStore {
    Tcl_HashTable byName;
} BindStore;

typedef struct BindStoreMap {
    Tcl_HashTable byStmt;
} BindStoreMap;

#define BINDSTORE_ASSOC "oradpi.bindstore"

static void BindStoreDelete(void *cd, Tcl_Interp *ip) {
    (void)ip;
    BindStoreMap *bm = (BindStoreMap *)cd;
    if (!bm)
        return;

    Tcl_HashSearch hs1;
    for (Tcl_HashEntry *e1 = Tcl_FirstHashEntry(&bm->byStmt, &hs1); e1; e1 = Tcl_NextHashEntry(&hs1)) {
        BindStore *bs = (BindStore *)Tcl_GetHashValue(e1);
        if (!bs)
            continue;
        Tcl_HashSearch hs2;
        for (Tcl_HashEntry *e2 = Tcl_FirstHashEntry(&bs->byName, &hs2); e2; e2 = Tcl_NextHashEntry(&hs2)) {
            Tcl_Obj *v = (Tcl_Obj *)Tcl_GetHashValue(e2);
            if (v)
                Tcl_DecrRefCount(v);
        }
        Tcl_DeleteHashTable(&bs->byName);
        Tcl_Free((char *)bs);
    }
    Tcl_DeleteHashTable(&bm->byStmt);
    Tcl_Free((char *)bm);
}

static BindStoreMap *GetBindStoreMap(Tcl_Interp *ip) {
    BindStoreMap *bm = (BindStoreMap *)Tcl_GetAssocData(ip, BINDSTORE_ASSOC, NULL);
    if (bm)
        return bm;
    bm = (BindStoreMap *)Tcl_Alloc(sizeof(*bm));
    Tcl_InitHashTable(&bm->byStmt, TCL_STRING_KEYS);
    Tcl_SetAssocData(ip, BINDSTORE_ASSOC, BindStoreDelete, bm);
    return bm;
}

static BindStore *GetBindStore(Tcl_Interp *ip, const char *stmtKey) {
    BindStoreMap  *bm    = GetBindStoreMap(ip);
    int            isNew = 0;
    Tcl_HashEntry *he    = Tcl_CreateHashEntry(&bm->byStmt, stmtKey, &isNew);
    BindStore     *bs    = isNew ? NULL : (BindStore *)Tcl_GetHashValue(he);
    if (!bs) {
        bs = (BindStore *)Tcl_Alloc(sizeof(*bs));
        Tcl_InitHashTable(&bs->byName, TCL_STRING_KEYS);
        Tcl_SetHashValue(he, bs);
    }
    return bs;
}

static void StoreBind(BindStore *bs, const char *nameNoColon, Tcl_Obj *v) {
    int            isNew = 0;
    Tcl_HashEntry *he    = Tcl_CreateHashEntry(&bs->byName, nameNoColon, &isNew);
    if (!isNew) {
        Tcl_Obj *ov = (Tcl_Obj *)Tcl_GetHashValue(he);
        if (ov)
            Tcl_DecrRefCount(ov);
    }
    Tcl_IncrRefCount(v);
    Tcl_SetHashValue(he, v);
}

void Oradpi_BindStoreForget(Tcl_Interp *ip, const char *stmtKey) {
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

typedef struct PendingMap {
    Tcl_HashTable byStmt;
} PendingMap;

#define PENDING_ASSOC "oradpi.pending"

static void PendingDelete(void *cd, Tcl_Interp *ip) {
    (void)ip;
    PendingMap *pm = (PendingMap *)cd;
    if (!pm)
        return;

    Tcl_HashSearch hs;
    for (Tcl_HashEntry *e = Tcl_FirstHashEntry(&pm->byStmt, &hs); e; e = Tcl_NextHashEntry(&hs)) {
        PendingRefs *pr = (PendingRefs *)Tcl_GetHashValue(e);
        if (!pr)
            continue;
        for (int i = 0; i < pr->n; i++)
            if (pr->vars[i])
                dpiVar_release(pr->vars[i]);
        if (pr->vars)
            Tcl_Free((char *)pr->vars);
        Tcl_Free((char *)pr);
    }
    Tcl_DeleteHashTable(&pm->byStmt);
    Tcl_Free((char *)pm);
}

static PendingMap *GetPendingMap(Tcl_Interp *ip) {
    PendingMap *pm = (PendingMap *)Tcl_GetAssocData(ip, PENDING_ASSOC, NULL);
    if (pm)
        return pm;
    pm = (PendingMap *)Tcl_Alloc(sizeof(*pm));
    Tcl_InitHashTable(&pm->byStmt, TCL_STRING_KEYS);
    Tcl_SetAssocData(ip, PENDING_ASSOC, PendingDelete, pm);
    return pm;
}

static PendingRefs *GetPendings(Tcl_Interp *ip, const char *stmtKey) {
    PendingMap    *pm    = GetPendingMap(ip);
    int            isNew = 0;
    Tcl_HashEntry *he    = Tcl_CreateHashEntry(&pm->byStmt, stmtKey, &isNew);
    PendingRefs   *pr    = isNew ? NULL : (PendingRefs *)Tcl_GetHashValue(he);
    if (!pr) {
        pr       = (PendingRefs *)Tcl_Alloc(sizeof(*pr));
        pr->n    = 0;
        pr->cap  = 4;
        pr->vars = (dpiVar **)Tcl_Alloc(sizeof(dpiVar *) * pr->cap);
        Tcl_SetHashValue(he, pr);
    }
    return pr;
}

static void Pending_Add(PendingRefs *pr, dpiVar *v) {
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
    pr->n = 0;
}

void Oradpi_PendingsForget(Tcl_Interp *ip, const char *stmtKey) {
    PendingMap *pm = (PendingMap *)Tcl_GetAssocData(ip, PENDING_ASSOC, NULL);
    if (!pm)
        return;
    Tcl_HashEntry *he = Tcl_FindHashEntry(&pm->byStmt, stmtKey);
    if (!he)
        return;

    PendingRefs *pr = (PendingRefs *)Tcl_GetHashValue(he);
    if (pr) {
        for (int i = 0; i < pr->n; i++)
            if (pr->vars[i])
                dpiVar_release(pr->vars[i]);
        if (pr->vars)
            Tcl_Free((char *)pr->vars);
        Tcl_Free((char *)pr);
    }
    Tcl_DeleteHashEntry(he);
}

int BindVarByNameDual(OradpiStmt *s, const char *nameNoColon, dpiVar *var, Tcl_Interp *ip, const char *ctx) {
    char     buf[256];
    uint32_t m = with_colon(nameNoColon, buf, (uint32_t)sizeof(buf));
    if (m && dpiStmt_bindByName(s->stmt, buf, m, var) == DPI_SUCCESS)
        return TCL_OK;

    uint32_t nlen = (uint32_t)strlen(nameNoColon);
    if (dpiStmt_bindByName(s->stmt, nameNoColon, nlen, var) == DPI_SUCCESS)
        return TCL_OK;

    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, ctx);
}

int BindValueByNameDual(OradpiStmt *s, const char *nameNoColon, dpiNativeTypeNum ntn, dpiData *d, Tcl_Interp *ip, const char *ctx) {
    char     buf[256];
    uint32_t m = with_colon(nameNoColon, buf, (uint32_t)sizeof(buf));
    if (m && dpiStmt_bindValueByName(s->stmt, buf, m, ntn, d) == DPI_SUCCESS)
        return TCL_OK;

    uint32_t nlen = (uint32_t)strlen(nameNoColon);
    if (dpiStmt_bindValueByName(s->stmt, nameNoColon, nlen, ntn, d) == DPI_SUCCESS)
        return TCL_OK;

    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, ctx);
}

static int BindOneLobScalar_Pending(Tcl_Interp *ip, OradpiStmt *s, PendingRefs *pr, const char *nameNoColon, dpiOracleTypeNum lobType, const char *buf, uint32_t buflen) {
    (void)pr;

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

    PendingRefs *use = GetPendings(ip, Tcl_GetString(s->base.name));
    Pending_Add(use, var);
    return TCL_OK;
}

static int BindOneByValue_Pending(Tcl_Interp *ip, OradpiStmt *s, PendingRefs *pr, const char *nameNoColon, Tcl_Obj *valueObj) {
    if (is_blob_hint(nameNoColon)) {
        Tcl_Size             blen = 0;
        const unsigned char *bp   = NULL;

        if (valueObj->typePtr == Tcl_GetObjType("bytearray")) {
            bp = Tcl_GetByteArrayFromObj(valueObj, &blen);
            return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char *)bp, (uint32_t)blen);
        } else {
            const char *sv = Tcl_GetStringFromObj(valueObj, &blen);
            return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, (uint32_t)blen);
        }
    }

    if (valueObj->typePtr == Tcl_GetObjType("bytearray")) {
        Tcl_Size       blen = 0;
        unsigned char *b    = Tcl_GetByteArrayFromObj(valueObj, &blen);
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char *)b, (uint32_t)blen);
    }

    Tcl_Size    sl = 0;
    const char *sv = Tcl_GetStringFromObj(valueObj, &sl);

    if (sl > 0 && memchr(sv, '\0', (size_t)sl) != NULL) {
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, (uint32_t)sl);
    }

    if (is_clob_hint(nameNoColon) && sl > 0) {
        if (sl > 4000)
            return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, (uint32_t)sl);
    }

    if (sl > 4000) {
        return BindOneLobScalar_Pending(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, (uint32_t)sl);
    }

    dpiData d;
    memset(&d, 0, sizeof d);
    Tcl_WideInt wi;
    double      dd;
    if (Tcl_GetWideIntFromObj(NULL, valueObj, &wi) == TCL_OK) {
        d.value.asInt64 = (int64_t)wi;
        return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_INT64, &d, ip, "dpiStmt_bindValueByName(int64)");
    }
    if (Tcl_GetDoubleFromObj(NULL, valueObj, &dd) == TCL_OK) {
        d.value.asDouble = dd;
        return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_DOUBLE, &d, ip, "dpiStmt_bindValueByName(double)");
    }

    dpiEncodingInfo enc;
    memset(&enc, 0, sizeof enc);
    (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);
    d.value.asBytes.ptr      = (char *)sv;
    d.value.asBytes.length   = (uint32_t)sl;
    d.value.asBytes.encoding = enc.encoding;
    return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_BYTES, &d, ip, "dpiStmt_bindValueByName(bytes)");
}

int Oradpi_Cmd_Orabind(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle :name value ? :name value ... ?");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    const char  *stmtKey = Tcl_GetString(objv[1]);
    PendingRefs *pr      = GetPendings(ip, stmtKey);
    Pendings_ReleaseAll(pr);
    BindStore *bs  = GetBindStore(ip, stmtKey);

    Tcl_Size   i   = 2;
    int        saw = 0;
    while (i + 1 < objc && Tcl_GetString(objv[i])[0] == ':') {
        const char *nameNoColon = strip_colon(Tcl_GetString(objv[i]), NULL);
        Tcl_Obj    *val         = objv[i + 1];

        if (BindOneByValue_Pending(ip, s, pr, nameNoColon, val) != TCL_OK)
            return TCL_ERROR;

        StoreBind(bs, nameNoColon, val);
        i += 2;
        saw = 1;
    }
    if (!saw) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle :name value ? :name value ... ?");
        return TCL_ERROR;
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Orabindexec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit? ?-arraydml? :name value|list ...");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");

    int      doCommit = 0;
    int      arrayDml = 0;

    Tcl_Size i        = 2;
    while (i < objc) {
        const char *opt = Tcl_GetString(objv[i]);
        if (strcmp(opt, "-commit") == 0) {
            doCommit = 1;
            i++;
            continue;
        }
        if (strcmp(opt, "-arraydml") == 0) {
            arrayDml = 1;
            i++;
            continue;
        }
        break;
    }

    if (arrayDml) {
        typedef struct ArrSpec {
            const char      *nameNoColon;
            Tcl_Obj         *listObj;
            Tcl_Obj        **elems;
            Tcl_Size         count;

            dpiOracleTypeNum ora;
            dpiNativeTypeNum nat;

            uint32_t         elemSize;
            int              sizeIsBytes;

            dpiVar          *var;
            dpiData         *data;

            char           **ownedBufs;
        } ArrSpec;

        int      cap = 8, nSpecs = 0;
        ArrSpec *specs    = (ArrSpec *)Tcl_Alloc(sizeof(ArrSpec) * cap);

        Tcl_Size expected = -1;

        Tcl_Size j        = i;
        while (j + 1 < objc && Tcl_GetString(objv[j])[0] == ':') {
            if (nSpecs == cap) {
                cap *= 2;
                specs = (ArrSpec *)Tcl_Realloc((char *)specs, sizeof(ArrSpec) * cap);
            }
            ArrSpec *as = &specs[nSpecs];
            memset(as, 0, sizeof(*as));

            as->nameNoColon = strip_colon(Tcl_GetString(objv[j]), NULL);
            as->listObj     = objv[j + 1];
            Tcl_IncrRefCount(as->listObj);

            if (Tcl_ListObjGetElements(ip, as->listObj, &as->count, &as->elems) != TCL_OK) {
                for (int t = 0; t <= nSpecs; t++)
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                Tcl_Free((char *)specs);
                return TCL_ERROR;
            }

            if (expected < 0)
                expected = as->count;
            if (as->count != expected) {
                Tcl_Obj *msg = Tcl_NewStringObj("-arraydml list lengths mismatch: :", -1);
                Tcl_AppendToObj(msg, as->nameNoColon, -1);
                Tcl_AppendPrintfToObj(msg, " has %ld vs expected %ld", (long)as->count, (long)expected);
                for (int t = 0; t <= nSpecs; t++)
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                Tcl_Free((char *)specs);
                Tcl_SetObjResult(ip, msg);
                return TCL_ERROR;
            }

            as->ora         = DPI_ORACLE_TYPE_VARCHAR;
            as->nat         = DPI_NATIVE_TYPE_BYTES;
            as->elemSize    = 1;
            as->sizeIsBytes = 1;
            for (Tcl_Size p = 0; p < as->count; p++) {
                Tcl_WideInt wi;
                double      dd;
                if (Tcl_GetWideIntFromObj(NULL, as->elems[p], &wi) == TCL_OK) {
                    as->ora = DPI_ORACLE_TYPE_NUMBER;
                    as->nat = DPI_NATIVE_TYPE_INT64;
                    break;
                }
                if (Tcl_GetDoubleFromObj(NULL, as->elems[p], &dd) == TCL_OK) {
                    as->ora = DPI_ORACLE_TYPE_NUMBER;
                    as->nat = DPI_NATIVE_TYPE_DOUBLE;
                    break;
                }
                Tcl_Size sl = 0;
                (void)Tcl_GetStringFromObj(as->elems[p], &sl);
                if ((uint32_t)sl > as->elemSize)
                    as->elemSize = (uint32_t)sl;
            }
            if (as->nat == DPI_NATIVE_TYPE_BYTES && as->elemSize == 0)
                as->elemSize = 1;

            nSpecs++;
            j += 2;
        }

        if (nSpecs == 0) {
            Tcl_SetResult(ip, "orabindexec -arraydml requires :name list pairs", TCL_STATIC);
            return TCL_ERROR;
        }

        uint32_t        iters = (uint32_t)expected;

        dpiEncodingInfo enc;
        memset(&enc, 0, sizeof enc);
        (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);

        for (int k = 0; k < nSpecs; k++) {
            ArrSpec *as = &specs[k];

            if (dpiConn_newVar(s->owner->conn, as->ora, as->nat, iters, (as->nat == DPI_NATIVE_TYPE_BYTES) ? as->elemSize : 0, (as->nat == DPI_NATIVE_TYPE_BYTES), 0, NULL, &as->var, &as->data) !=
                DPI_SUCCESS) {
                for (int t = 0; t < nSpecs; t++)
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                Tcl_Free((char *)specs);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_newVar(array)");
            }

            if (as->nat == DPI_NATIVE_TYPE_BYTES) {
                as->ownedBufs = (char **)Tcl_Alloc(sizeof(char *) * iters);
                memset(as->ownedBufs, 0, sizeof(char *) * iters);
            } else {
                as->ownedBufs = NULL;
            }

            for (uint32_t r = 0; r < iters; r++) {
                Tcl_Obj *e = as->elems[r];
                if (as->nat == DPI_NATIVE_TYPE_INT64) {
                    Tcl_WideInt wi;
                    (void)Tcl_GetWideIntFromObj(NULL, e, &wi);
                    as->data[r].isNull        = 0;
                    as->data[r].value.asInt64 = (int64_t)wi;
                } else if (as->nat == DPI_NATIVE_TYPE_DOUBLE) {
                    double dd;
                    (void)Tcl_GetDoubleFromObj(NULL, e, &dd);
                    as->data[r].isNull         = 0;
                    as->data[r].value.asDouble = dd;
                } else {
                    Tcl_Size    sl  = 0;
                    const char *sv  = Tcl_GetStringFromObj(e, &sl);
                    char       *buf = NULL;
                    if (sl > 0) {
                        buf = (char *)Tcl_Alloc((size_t)sl);
                        memcpy(buf, sv, (size_t)sl);
                    } else {
                        buf = (char *)Tcl_Alloc(1);
                    }
                    as->ownedBufs[r]                   = buf;
                    as->data[r].isNull                 = 0;
                    as->data[r].value.asBytes.ptr      = buf;
                    as->data[r].value.asBytes.length   = (uint32_t)sl;
                    as->data[r].value.asBytes.encoding = enc.encoding;
                }
            }

            if (BindVarByNameDual(s, as->nameNoColon, as->var, ip, "dpiStmt_bindByName(array)") != TCL_OK) {
                for (int t = 0; t < nSpecs; t++) {
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                }
                Tcl_Free((char *)specs);
                return TCL_ERROR;
            }
        }

        dpiStmtInfo info;
        if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
            for (int t = 0; t < nSpecs; t++)
                if (specs[t].listObj)
                    Tcl_DecrRefCount(specs[t].listObj);
            Tcl_Free((char *)specs);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getInfo");
        }

        dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
        if (info.isDML)
            mode |= DPI_MODE_EXEC_BATCH_ERRORS;
        if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
            mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

        if (dpiStmt_executeMany(s->stmt, mode, iters) != DPI_SUCCESS) {
            for (int t = 0; t < nSpecs; t++)
                if (specs[t].listObj)
                    Tcl_DecrRefCount(specs[t].listObj);
            Tcl_Free((char *)specs);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_executeMany");
        }

        uint64_t rows = 0;
        if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS) {
            Oradpi_RecordRows((OradpiBase *)s, rows);
        }

        for (int k = 0; k < nSpecs; k++) {
            ArrSpec *as = &specs[k];
            if (as->var)
                dpiVar_release(as->var);
            if (as->ownedBufs) {
                for (uint32_t r = 0; r < iters; r++)
                    if (as->ownedBufs[r])
                        Tcl_Free(as->ownedBufs[r]);
                Tcl_Free((char *)as->ownedBufs);
            }
            Tcl_DecrRefCount(as->listObj);
        }
        Tcl_Free((char *)specs);

        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    const char  *stmtKey = Tcl_GetString(objv[1]);
    PendingRefs *pr      = GetPendings(ip, stmtKey);
    BindStore   *bs      = GetBindStore(ip, stmtKey);

    Tcl_Size     k       = i;
    while (k + 1 < objc && Tcl_GetString(objv[k])[0] == ':') {
        const char *nameNoColon = strip_colon(Tcl_GetString(objv[k]), NULL);
        Tcl_Obj    *val         = objv[k + 1];

        if (BindOneByValue_Pending(ip, s, pr, nameNoColon, val) != TCL_OK)
            return TCL_ERROR;

        StoreBind(bs, nameNoColon, val);
        k += 2;
    }

    dpiStmtInfo info;
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
        Pendings_ReleaseAll(pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getInfo");
    }

    dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
    if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
        mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

    uint32_t nqc = 0;
    if (dpiStmt_execute(s->stmt, mode, &nqc) != DPI_SUCCESS) {
        Pendings_ReleaseAll(pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_execute");
    }

    uint64_t rows = 0;
    if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS) {
        Oradpi_RecordRows((OradpiBase *)s, rows);
    }
    Oradpi_UpdateStmtType(s);

    Pendings_ReleaseAll(pr);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
