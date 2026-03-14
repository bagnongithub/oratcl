/*
 *  cmd_bind.c --
 *
 *    Bind variable creation and management for statements.
 *    This is the CANONICAL bind infrastructure shared by cmd_exec.c.
 *
 *        - Uses ODPI-C typed variables and buffers; supports array DML and name/position binds.
 *        - Per-statement caches live inside the current interpreter; no global mutable state.
 *
 *  Copyright (c) 2025 Miguel Bañón.
 *
 *  See the file "license.terms" for information on usage and redistribution,
 *  and for a DISCLAIMER OF ALL WARRANTIES.
 *
 */

#include <limits.h>
#include <string.h>

#include "cmd_int.h"
#include "dpi.h"

typedef struct BindStore
{
    Tcl_HashTable byName;
} BindStore;

typedef struct BindStoreMap
{
    Tcl_HashTable byStmt;
} BindStoreMap;

typedef struct PendingMap
{
    Tcl_HashTable byStmt;
} PendingMap;

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static int BindOneLobScalar(Tcl_Interp* ip,
                            OradpiStmt* s,
                            OradpiPendingRefs* pr,
                            const char* nameNoColon,
                            dpiOracleTypeNum lobType,
                            const char* buf,
                            uint32_t buflen);
static void BindStoreDelete(void* cd, Tcl_Interp* ip);
static BindStore* GetBindStore(Tcl_Interp* ip, const char* stmtKey);
static BindStoreMap* GetBindStoreMap(Tcl_Interp* ip);
static PendingMap* GetPendingMap(Tcl_Interp* ip);
static OradpiPendingRefs* GetPendings(Tcl_Interp* ip, const char* stmtKey);
static int is_blob_hint(const char* nameNoColon);
static int is_clob_hint(const char* nameNoColon);
static void PendingDelete(void* cd, Tcl_Interp* ip);
static void StoreBind(BindStore* bs, const char* nameNoColon, Tcl_Obj* v);
static int strcasestr_contains(const char* hay, const char* needle);

/* Cached Tcl type pointer to avoid per-bind hash lookup (fix 4.3) */
static const Tcl_ObjType* gBytearrayType = NULL;
/* gTypeInitMutex: protects gBytearrayType (one-time init).
 * Lock ordering: leaf lock, no other locks held while this is held. */
static Tcl_Mutex gTypeInitMutex;

/* Safely narrow Tcl_Size to uint32_t for ODPI-C APIs; returns TCL_ERROR on overflow */
static int CheckU32(Tcl_Interp* ip, Tcl_Size len, uint32_t* out)
{
    if (len < 0 || (uint64_t)len > UINT32_MAX)
    {
        Tcl_SetObjResult(ip, Tcl_NewStringObj("value length exceeds uint32_t range for ODPI-C", -1));
        return TCL_ERROR;
    }
    *out = (uint32_t)len;
    return TCL_OK;
}

static const Tcl_ObjType* EnsureBytearrayType(void)
{
    if (!gBytearrayType)
    {
        Tcl_MutexLock(&gTypeInitMutex);
        if (!gBytearrayType)
        {
            gBytearrayType = Tcl_GetObjType("bytearray");
            /* Fallback: force Tcl to register the type by creating a temp obj */
            if (!gBytearrayType)
            {
                Tcl_Obj* tmp = Tcl_NewByteArrayObj(NULL, 0);
                Tcl_IncrRefCount(tmp);
                gBytearrayType = Tcl_GetObjType("bytearray");
                Tcl_DecrRefCount(tmp);
            }
        }
        Tcl_MutexUnlock(&gTypeInitMutex);
    }
    return gBytearrayType;
}

#ifdef _WIN32
#include <ctype.h>
static char* strcasestr(const char* haystack, const char* needle)
{
    if (!haystack || !needle)
        return NULL;
    size_t needle_len = strlen(needle);
    if (needle_len == 0)
        return (char*)haystack;
    for (; *haystack; haystack++)
    {
        if (_strnicmp(haystack, needle, needle_len) == 0)
            return (char*)haystack;
    }
    return NULL;
}
#endif

/* ------------------------------------------------------------------------- *
 * Shared utility functions (exported via cmd_int.h)
 * ------------------------------------------------------------------------- */

uint32_t Oradpi_WithColon(const char* nameNoColon, char* dst, uint32_t cap)
{
    if (!nameNoColon)
        nameNoColon = "";
    size_t n = strlen(nameNoColon);
    /* Overflow-safe check: need n+2 bytes (colon + name + NUL) */
    if (n > (size_t)UINT32_MAX - 2 || (size_t)cap < n + 2)
        return 0;
    dst[0] = ':';
    memcpy(dst + 1, nameNoColon, n);
    dst[n + 1] = '\0';
    return (uint32_t)(n + 1);
}

const char* Oradpi_StripColon(const char* raw, uint32_t* nlenOut)
{
    const char* p = raw ? raw : "";
    if (*p == ':')
        p++;
    if (nlenOut)
    {
        size_t len = strlen(p);
        *nlenOut = (len <= UINT32_MAX) ? (uint32_t)len : UINT32_MAX;
    }
    return p;
}

/* ---- Bind name resolution (consistent order: colon-prefixed first) ---- */

static int BindVarByNameDual(OradpiStmt* s, const char* nameNoColon, dpiVar* var, Tcl_Interp* ip, const char* ctx)
{
    char buf[256];
    uint32_t m = Oradpi_WithColon(nameNoColon, buf, (uint32_t)sizeof(buf));
    if (m && dpiStmt_bindByName(s->stmt, buf, m, var) == DPI_SUCCESS)
        return TCL_OK;

    uint32_t nlen = (uint32_t)strlen(nameNoColon);
    if (dpiStmt_bindByName(s->stmt, nameNoColon, nlen, var) == DPI_SUCCESS)
        return TCL_OK;

    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, ctx);
}

static int
BindValueByNameDual(OradpiStmt* s, const char* nameNoColon, dpiNativeTypeNum ntn, dpiData* d, Tcl_Interp* ip, const char* ctx)
{
    char buf[256];
    uint32_t m = Oradpi_WithColon(nameNoColon, buf, (uint32_t)sizeof(buf));
    if (m && dpiStmt_bindValueByName(s->stmt, buf, m, ntn, d) == DPI_SUCCESS)
        return TCL_OK;

    uint32_t nlen = (uint32_t)strlen(nameNoColon);
    if (dpiStmt_bindValueByName(s->stmt, nameNoColon, nlen, ntn, d) == DPI_SUCCESS)
        return TCL_OK;

    return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, ctx);
}

/* ---- Hint helpers ---- */

static int strcasestr_contains(const char* hay, const char* needle)
{
    if (!hay || !needle || !*needle)
        return 0;
    size_t nl = strlen(needle);
    for (const char* p = hay; *p; ++p)
    {
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

static int is_blob_hint(const char* nameNoColon)
{
    if (!nameNoColon || !*nameNoColon)
        return 0;
    if ((nameNoColon[0] == 'b' || nameNoColon[0] == 'B') && nameNoColon[1] == '\0')
        return 1;
    return strcasestr_contains(nameNoColon, "blob");
}
static int is_clob_hint(const char* nameNoColon)
{
    if (!nameNoColon || !*nameNoColon)
        return 0;
    if ((nameNoColon[0] == 'c' || nameNoColon[0] == 'C') && nameNoColon[1] == '\0')
        return 1;
    return strcasestr_contains(nameNoColon, "clob");
}

/* ---- BindStore (per-statement stored binds) ---- */

static void BindStoreDelete(void* cd, Tcl_Interp* ip)
{
    (void)ip;
    BindStoreMap* bm = (BindStoreMap*)cd;
    if (!bm)
        return;
    Tcl_HashSearch hs1;
    for (Tcl_HashEntry* e1 = Tcl_FirstHashEntry(&bm->byStmt, &hs1); e1; e1 = Tcl_NextHashEntry(&hs1))
    {
        BindStore* bs = (BindStore*)Tcl_GetHashValue(e1);
        if (!bs)
            continue;
        Tcl_HashSearch hs2;
        for (Tcl_HashEntry* e2 = Tcl_FirstHashEntry(&bs->byName, &hs2); e2; e2 = Tcl_NextHashEntry(&hs2))
        {
            Tcl_Obj* v = (Tcl_Obj*)Tcl_GetHashValue(e2);
            if (v)
                Tcl_DecrRefCount(v);
        }
        Tcl_DeleteHashTable(&bs->byName);
        Tcl_Free((char*)bs);
    }
    Tcl_DeleteHashTable(&bm->byStmt);
    Tcl_Free((char*)bm);
}

static BindStoreMap* GetBindStoreMap(Tcl_Interp* ip)
{
    BindStoreMap* bm = (BindStoreMap*)Tcl_GetAssocData(ip, BINDSTORE_ASSOC, NULL);
    if (bm)
        return bm;
    bm = (BindStoreMap*)Tcl_Alloc(sizeof(*bm));
    Tcl_InitHashTable(&bm->byStmt, TCL_STRING_KEYS);
    Tcl_SetAssocData(ip, BINDSTORE_ASSOC, BindStoreDelete, bm);
    return bm;
}

static BindStore* GetBindStore(Tcl_Interp* ip, const char* stmtKey)
{
    BindStoreMap* bm = GetBindStoreMap(ip);
    int isNew = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&bm->byStmt, stmtKey, &isNew);
    BindStore* bs = isNew ? NULL : (BindStore*)Tcl_GetHashValue(he);
    if (!bs)
    {
        bs = (BindStore*)Tcl_Alloc(sizeof(*bs));
        Tcl_InitHashTable(&bs->byName, TCL_STRING_KEYS);
        Tcl_SetHashValue(he, bs);
    }
    return bs;
}

static void StoreBind(BindStore* bs, const char* nameNoColon, Tcl_Obj* v)
{
    int isNew = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&bs->byName, nameNoColon, &isNew);
    if (!isNew)
    {
        Tcl_Obj* ov = (Tcl_Obj*)Tcl_GetHashValue(he);
        if (ov)
            Tcl_DecrRefCount(ov);
    }
    Tcl_IncrRefCount(v);
    Tcl_SetHashValue(he, v);
}

void Oradpi_BindStoreForget(Tcl_Interp* ip, const char* stmtKey)
{
    BindStoreMap* bm = (BindStoreMap*)Tcl_GetAssocData(ip, BINDSTORE_ASSOC, NULL);
    if (!bm)
        return;
    Tcl_HashEntry* he = Tcl_FindHashEntry(&bm->byStmt, stmtKey);
    if (!he)
        return;
    BindStore* bs = (BindStore*)Tcl_GetHashValue(he);
    if (bs)
    {
        Tcl_HashSearch hs;
        for (Tcl_HashEntry* e = Tcl_FirstHashEntry(&bs->byName, &hs); e; e = Tcl_NextHashEntry(&hs))
        {
            Tcl_Obj* v = (Tcl_Obj*)Tcl_GetHashValue(e);
            if (v)
                Tcl_DecrRefCount(v);
        }
        Tcl_DeleteHashTable(&bs->byName);
        Tcl_Free((char*)bs);
    }
    Tcl_DeleteHashEntry(he);
}

void Oradpi_ClearBindStoreForStmt(Tcl_Interp* ip, const char* stmtKey)
{
    Oradpi_BindStoreForget(ip, stmtKey);
}

/* ---- PendingRefs (dpiVar* refs kept alive until execution) ---- */

static void PendingDelete(void* cd, Tcl_Interp* ip)
{
    (void)ip;
    PendingMap* pm = (PendingMap*)cd;
    if (!pm)
        return;
    Tcl_HashSearch hs;
    for (Tcl_HashEntry* e = Tcl_FirstHashEntry(&pm->byStmt, &hs); e; e = Tcl_NextHashEntry(&hs))
    {
        OradpiPendingRefs* pr = (OradpiPendingRefs*)Tcl_GetHashValue(e);
        if (!pr)
            continue;
        for (Tcl_Size i = 0; i < pr->n; i++)
            if (pr->vars[i])
                dpiVar_release(pr->vars[i]);
        if (pr->vars)
            Tcl_Free((char*)pr->vars);
        Tcl_Free((char*)pr);
    }
    Tcl_DeleteHashTable(&pm->byStmt);
    Tcl_Free((char*)pm);
}

static PendingMap* GetPendingMap(Tcl_Interp* ip)
{
    PendingMap* pm = (PendingMap*)Tcl_GetAssocData(ip, PENDING_ASSOC, NULL);
    if (pm)
        return pm;
    pm = (PendingMap*)Tcl_Alloc(sizeof(*pm));
    Tcl_InitHashTable(&pm->byStmt, TCL_STRING_KEYS);
    Tcl_SetAssocData(ip, PENDING_ASSOC, PendingDelete, pm);
    return pm;
}

static OradpiPendingRefs* GetPendings(Tcl_Interp* ip, const char* stmtKey)
{
    PendingMap* pm = GetPendingMap(ip);
    int isNew = 0;
    Tcl_HashEntry* he = Tcl_CreateHashEntry(&pm->byStmt, stmtKey, &isNew);
    OradpiPendingRefs* pr = isNew ? NULL : (OradpiPendingRefs*)Tcl_GetHashValue(he);
    if (!pr)
    {
        pr = (OradpiPendingRefs*)Tcl_Alloc(sizeof(*pr));
        pr->n = 0;
        pr->cap = 4;
        pr->vars = (dpiVar**)Tcl_Alloc(sizeof(dpiVar*) * pr->cap);
        Tcl_SetHashValue(he, pr);
    }
    return pr;
}

/* Public PendingRefs management for stack-local use (cmd_exec.c) */
void Oradpi_PendingsInit(OradpiPendingRefs* pr)
{
    pr->n = 0;
    pr->cap = 8;
    pr->vars = (dpiVar**)Tcl_Alloc(sizeof(dpiVar*) * pr->cap);
}

void Oradpi_PendingsAdd(OradpiPendingRefs* pr, dpiVar* v)
{
    if (pr->n == pr->cap)
    {
        pr->cap *= 2;
        pr->vars = (dpiVar**)Tcl_Realloc((char*)pr->vars, sizeof(dpiVar*) * pr->cap);
    }
    pr->vars[pr->n++] = v;
}

void Oradpi_PendingsReleaseAll(OradpiPendingRefs* pr)
{
    for (Tcl_Size i = 0; i < pr->n; i++)
        if (pr->vars[i])
            dpiVar_release(pr->vars[i]);
    pr->n = 0;
}

void Oradpi_PendingsFree(OradpiPendingRefs* pr)
{
    Oradpi_PendingsReleaseAll(pr);
    if (pr->vars)
    {
        Tcl_Free((char*)pr->vars);
        pr->vars = NULL;
    }
    pr->cap = 0;
}

void Oradpi_PendingsForget(Tcl_Interp* ip, const char* stmtKey)
{
    PendingMap* pm = (PendingMap*)Tcl_GetAssocData(ip, PENDING_ASSOC, NULL);
    if (!pm)
        return;
    Tcl_HashEntry* he = Tcl_FindHashEntry(&pm->byStmt, stmtKey);
    if (!he)
        return;
    OradpiPendingRefs* pr = (OradpiPendingRefs*)Tcl_GetHashValue(he);
    if (pr)
    {
        for (Tcl_Size i = 0; i < pr->n; i++)
            if (pr->vars[i])
                dpiVar_release(pr->vars[i]);
        if (pr->vars)
            Tcl_Free((char*)pr->vars);
        Tcl_Free((char*)pr);
    }
    Tcl_DeleteHashEntry(he);
}

/* ---- Core bind-by-value logic (shared) ---- */

static int BindOneLobScalar(Tcl_Interp* ip,
                            OradpiStmt* s,
                            OradpiPendingRefs* pr,
                            const char* nameNoColon,
                            dpiOracleTypeNum lobType,
                            const char* buf,
                            uint32_t buflen)
{
    dpiVar* var = NULL;
    dpiData* data = NULL;
    if (dpiConn_newVar(s->owner->conn, lobType, DPI_NATIVE_TYPE_LOB, 1, 0, 0, 0, NULL, &var, &data) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiConn_newVar(LOB)");

    dpiLob* lob = NULL;
    if (dpiConn_newTempLob(s->owner->conn, lobType, &lob) != DPI_SUCCESS)
    {
        dpiVar_release(var);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiConn_newTempLob");
    }
    if (buflen > 0)
    {
        if (dpiLob_setFromBytes(lob, buf, (uint64_t)buflen) != DPI_SUCCESS)
        {
            dpiLob_release(lob);
            dpiVar_release(var);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiLob_setFromBytes");
        }
    }
    if (dpiVar_setFromLob(var, 0, lob) != DPI_SUCCESS)
    {
        dpiLob_release(lob);
        dpiVar_release(var);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiVar_setFromLob");
    }
    dpiLob_release(lob);

    if (BindVarByNameDual(s, nameNoColon, var, ip, "dpiStmt_bindByName(LOB)") != TCL_OK)
    {
        dpiVar_release(var);
        return TCL_ERROR;
    }

    /* Use the passed pending refs (caller decides lifetime) */
    Oradpi_PendingsAdd(pr, var);
    return TCL_OK;
}

int Oradpi_BindOneByValue(Tcl_Interp* ip, OradpiStmt* s, OradpiPendingRefs* pr, const char* nameNoColon, Tcl_Obj* valueObj)
{
    if (is_blob_hint(nameNoColon))
    {
        Tcl_Size blen = 0;
        const unsigned char* bp = NULL;
        uint32_t blen32 = 0;
        if (valueObj->typePtr == EnsureBytearrayType())
        {
            bp = Tcl_GetByteArrayFromObj(valueObj, &blen);
            if (CheckU32(ip, blen, &blen32) != TCL_OK)
                return TCL_ERROR;
            return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char*)bp, blen32);
        }
        else
        {
            const char* sv = Tcl_GetStringFromObj(valueObj, &blen);
            if (CheckU32(ip, blen, &blen32) != TCL_OK)
                return TCL_ERROR;
            return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, blen32);
        }
    }

    if (valueObj->typePtr == EnsureBytearrayType())
    {
        Tcl_Size blen = 0;
        uint32_t blen32 = 0;
        unsigned char* b = Tcl_GetByteArrayFromObj(valueObj, &blen);
        if (CheckU32(ip, blen, &blen32) != TCL_OK)
            return TCL_ERROR;
        return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char*)b, blen32);
    }

    Tcl_Size sl = 0;
    const char* sv = Tcl_GetStringFromObj(valueObj, &sl);

    if (sl > 0 && memchr(sv, '\0', (size_t)sl) != NULL)
    {
        uint32_t sl32 = 0;
        if (CheckU32(ip, sl, &sl32) != TCL_OK)
            return TCL_ERROR;
        return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, sl32);
    }

    if (is_clob_hint(nameNoColon) && sl > 0)
    {
        if (sl > 4000)
        {
            uint32_t sl32 = 0;
            if (CheckU32(ip, sl, &sl32) != TCL_OK)
                return TCL_ERROR;
            return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, sl32);
        }
    }

    if (sl > 4000)
    {
        uint32_t sl32 = 0;
        if (CheckU32(ip, sl, &sl32) != TCL_OK)
            return TCL_ERROR;
        return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, sl32);
    }

    dpiData d;
    memset(&d, 0, sizeof d);
    Tcl_WideInt wi;
    double dd;
    if (Tcl_GetWideIntFromObj(NULL, valueObj, &wi) == TCL_OK)
    {
        d.value.asInt64 = (int64_t)wi;
        return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_INT64, &d, ip, "dpiStmt_bindValueByName(int64)");
    }
    if (Tcl_GetDoubleFromObj(NULL, valueObj, &dd) == TCL_OK)
    {
        d.value.asDouble = dd;
        return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_DOUBLE, &d, ip, "dpiStmt_bindValueByName(double)");
    }

    dpiEncodingInfo enc;
    memset(&enc, 0, sizeof enc);
    /* Use cached encoding from connection to avoid per-bind ODPI call (fix 4.2) */
    enc.encoding = s->owner->cachedEncoding;
    if (!enc.encoding)
    {
        /* Fallback if cache not populated (e.g., adopted connection) */
        (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);
    }
    uint32_t sl32 = 0;
    if (CheckU32(ip, sl, &sl32) != TCL_OK)
        return TCL_ERROR;
    d.value.asBytes.ptr = (char*)sv;
    d.value.asBytes.length = sl32;
    d.value.asBytes.encoding = enc.encoding;
    return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_BYTES, &d, ip, "dpiStmt_bindValueByName(bytes)");
}

/* Rebind all stored binds for a statement (used by cmd_exec.c) */
int Oradpi_RebindAllStored(Tcl_Interp* ip, OradpiStmt* s, OradpiPendingRefs* pr, const char* stmtKey)
{
    BindStoreMap* bm = (BindStoreMap*)Tcl_GetAssocData(ip, BINDSTORE_ASSOC, NULL);
    if (!bm)
        return TCL_OK;
    Tcl_HashEntry* he = Tcl_FindHashEntry(&bm->byStmt, stmtKey);
    if (!he)
        return TCL_OK;
    BindStore* bs = (BindStore*)Tcl_GetHashValue(he);
    if (!bs)
        return TCL_OK;
    Tcl_HashSearch hs;
    for (Tcl_HashEntry* e = Tcl_FirstHashEntry(&bs->byName, &hs); e; e = Tcl_NextHashEntry(&hs))
    {
        const char* nameNoColon = (const char*)Tcl_GetHashKey(&bs->byName, e);
        Tcl_Obj* val = (Tcl_Obj*)Tcl_GetHashValue(e);
        if (!val)
            continue;
        if (Oradpi_BindOneByValue(ip, s, pr, nameNoColon, val) != TCL_OK)
            return TCL_ERROR;
    }
    return TCL_OK;
}

/* ---- Command implementations ---- */

int Oradpi_Cmd_Orabind(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle :name value ? :name value ... ?");
        return TCL_ERROR;
    }

    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is not prepared or connection closed");

    const char* stmtKey = Tcl_GetString(objv[1]);
    OradpiPendingRefs* pr = GetPendings(ip, stmtKey);
    Oradpi_PendingsReleaseAll(pr);
    BindStore* bs = GetBindStore(ip, stmtKey);

    Tcl_Size i = 2;
    int saw = 0;
    while (i + 1 < objc && Tcl_GetString(objv[i])[0] == ':')
    {
        const char* nameNoColon = Oradpi_StripColon(Tcl_GetString(objv[i]), NULL);
        Tcl_Obj* val = objv[i + 1];

        if (Oradpi_BindOneByValue(ip, s, pr, nameNoColon, val) != TCL_OK)
            return TCL_ERROR;

        StoreBind(bs, nameNoColon, val);
        i += 2;
        saw = 1;
    }
    if (!saw)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle :name value ? :name value ... ?");
        return TCL_ERROR;
    }

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

int Oradpi_Cmd_Orabindexec(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit? ?-arraydml? :name value|list ...");
        return TCL_ERROR;
    }

    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is not prepared or connection closed");

    int doCommit = 0;
    int arrayDml = 0;

    Tcl_Size i = 2;
    while (i < objc)
    {
        const char* opt = Tcl_GetString(objv[i]);
        if (strcmp(opt, "-commit") == 0)
        {
            doCommit = 1;
            i++;
            continue;
        }
        if (strcmp(opt, "-arraydml") == 0)
        {
            arrayDml = 1;
            i++;
            continue;
        }
        break;
    }

    if (arrayDml)
    {
        typedef struct ArrSpec
        {
            const char* nameNoColon;
            Tcl_Obj* listObj;
            Tcl_Obj** elems;
            Tcl_Size count;
            dpiOracleTypeNum ora;
            dpiNativeTypeNum nat;
            uint32_t elemSize;
            int sizeIsBytes;
            dpiVar* var;
            dpiData* data;
            char** ownedBufs;
        } ArrSpec;

        int cap = 8, nSpecs = 0;
        ArrSpec* specs = (ArrSpec*)Tcl_Alloc(sizeof(ArrSpec) * cap);
        Tcl_Size expected = -1;

        Tcl_Size j = i;
        while (j + 1 < objc && Tcl_GetString(objv[j])[0] == ':')
        {
            if (nSpecs == cap)
            {
                cap *= 2;
                specs = (ArrSpec*)Tcl_Realloc((char*)specs, sizeof(ArrSpec) * cap);
            }
            ArrSpec* as = &specs[nSpecs];
            memset(as, 0, sizeof(*as));

            as->nameNoColon = Oradpi_StripColon(Tcl_GetString(objv[j]), NULL);
            as->listObj = objv[j + 1];
            Tcl_IncrRefCount(as->listObj);

            if (Tcl_ListObjGetElements(ip, as->listObj, &as->count, &as->elems) != TCL_OK)
            {
                for (int t = 0; t <= nSpecs; t++)
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                Tcl_Free((char*)specs);
                return TCL_ERROR;
            }

            if (expected < 0)
                expected = as->count;
            if (as->count != expected)
            {
                Tcl_Obj* msg = Tcl_NewStringObj("-arraydml list lengths mismatch: :", -1);
                Tcl_AppendToObj(msg, as->nameNoColon, -1);
                Tcl_AppendPrintfToObj(msg, " has %ld vs expected %ld", (long)as->count, (long)expected);
                for (int t = 0; t <= nSpecs; t++)
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                Tcl_Free((char*)specs);
                Tcl_SetObjResult(ip, msg);
                return TCL_ERROR;
            }

            as->ora = DPI_ORACLE_TYPE_VARCHAR;
            as->nat = DPI_NATIVE_TYPE_BYTES;
            as->elemSize = 1;
            as->sizeIsBytes = 1;
            /* Scan ALL elements to determine type consistently (fix 3.6).
             * Only use numeric type if every element parses as that type. */
            int allInt = 1, allNumeric = 1;
            for (Tcl_Size p = 0; p < as->count; p++)
            {
                Tcl_WideInt wi;
                double dd;
                if (Tcl_GetWideIntFromObj(NULL, as->elems[p], &wi) != TCL_OK)
                    allInt = 0;
                if (allInt == 0 && Tcl_GetDoubleFromObj(NULL, as->elems[p], &dd) != TCL_OK)
                    allNumeric = 0;
                if (!allInt && !allNumeric)
                {
                    /* Remaining elements only matter for max size */
                    Tcl_Size sl = 0;
                    (void)Tcl_GetStringFromObj(as->elems[p], &sl);
                    if ((uint32_t)sl > as->elemSize)
                        as->elemSize = (uint32_t)sl;
                }
                else
                {
                    Tcl_Size sl = 0;
                    (void)Tcl_GetStringFromObj(as->elems[p], &sl);
                    if ((uint32_t)sl > as->elemSize)
                        as->elemSize = (uint32_t)sl;
                }
            }
            if (allInt)
            {
                as->ora = DPI_ORACLE_TYPE_NUMBER;
                as->nat = DPI_NATIVE_TYPE_INT64;
            }
            else if (allNumeric)
            {
                as->ora = DPI_ORACLE_TYPE_NUMBER;
                as->nat = DPI_NATIVE_TYPE_DOUBLE;
            }
            if (as->nat == DPI_NATIVE_TYPE_BYTES && as->elemSize == 0)
                as->elemSize = 1;

            nSpecs++;
            j += 2;
        }

        if (nSpecs == 0)
        {
            Tcl_Free((char*)specs);
            Tcl_SetObjResult(ip, Tcl_NewStringObj("orabindexec -arraydml requires :name list pairs", -1));
            return TCL_ERROR;
        }

        uint32_t iters = (uint32_t)expected;

        dpiEncodingInfo enc;
        memset(&enc, 0, sizeof enc);
        /* Use cached encoding (fix 4.2) */
        enc.encoding = s->owner->cachedEncoding;
        if (!enc.encoding)
            (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);

        for (int k = 0; k < nSpecs; k++)
        {
            ArrSpec* as = &specs[k];

            if (dpiConn_newVar(s->owner->conn,
                               as->ora,
                               as->nat,
                               iters,
                               (as->nat == DPI_NATIVE_TYPE_BYTES) ? as->elemSize : 0,
                               (as->nat == DPI_NATIVE_TYPE_BYTES),
                               0,
                               NULL,
                               &as->var,
                               &as->data) != DPI_SUCCESS)
            {
                /* Clean up already-created vars for specs[0..k-1] (fix 2.4) */
                for (int t = 0; t < k; t++)
                {
                    if (specs[t].var)
                        dpiVar_release(specs[t].var);
                    if (specs[t].ownedBufs)
                    {
                        for (uint32_t r2 = 0; r2 < iters; r2++)
                            if (specs[t].ownedBufs[r2])
                                Tcl_Free(specs[t].ownedBufs[r2]);
                        Tcl_Free((char*)specs[t].ownedBufs);
                    }
                }
                for (int t = 0; t < nSpecs; t++)
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                Tcl_Free((char*)specs);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiConn_newVar(array)");
            }

            if (as->nat == DPI_NATIVE_TYPE_BYTES)
            {
                as->ownedBufs = (char**)Tcl_Alloc(sizeof(char*) * iters);
                memset(as->ownedBufs, 0, sizeof(char*) * iters);
            }
            else
            {
                as->ownedBufs = NULL;
            }

            for (uint32_t r = 0; r < iters; r++)
            {
                Tcl_Obj* e = as->elems[r];
                if (as->nat == DPI_NATIVE_TYPE_INT64)
                {
                    Tcl_WideInt wi;
                    (void)Tcl_GetWideIntFromObj(NULL, e, &wi);
                    as->data[r].isNull = 0;
                    as->data[r].value.asInt64 = (int64_t)wi;
                }
                else if (as->nat == DPI_NATIVE_TYPE_DOUBLE)
                {
                    double dd;
                    (void)Tcl_GetDoubleFromObj(NULL, e, &dd);
                    as->data[r].isNull = 0;
                    as->data[r].value.asDouble = dd;
                }
                else
                {
                    Tcl_Size sl = 0;
                    const char* sv = Tcl_GetStringFromObj(e, &sl);
                    char* buf = NULL;
                    if (sl > 0)
                    {
                        buf = (char*)Tcl_Alloc((size_t)sl);
                        memcpy(buf, sv, (size_t)sl);
                    }
                    else
                    {
                        buf = (char*)Tcl_Alloc(1);
                    }
                    as->ownedBufs[r] = buf;
                    as->data[r].isNull = 0;
                    as->data[r].value.asBytes.ptr = buf;
                    as->data[r].value.asBytes.length = (uint32_t)sl;
                    as->data[r].value.asBytes.encoding = enc.encoding;
                }
            }

            if (BindVarByNameDual(s, as->nameNoColon, as->var, ip, "dpiStmt_bindByName(array)") != TCL_OK)
            {
                for (int t = 0; t < nSpecs; t++)
                {
                    if (specs[t].var)
                        dpiVar_release(specs[t].var);
                    if (specs[t].ownedBufs)
                    {
                        for (uint32_t r2 = 0; r2 < iters; r2++)
                            if (specs[t].ownedBufs[r2])
                                Tcl_Free(specs[t].ownedBufs[r2]);
                        Tcl_Free((char*)specs[t].ownedBufs);
                    }
                    if (specs[t].listObj)
                        Tcl_DecrRefCount(specs[t].listObj);
                }
                Tcl_Free((char*)specs);
                return TCL_ERROR;
            }
        }

        dpiStmtInfo info;
        if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS)
        {
            for (int t = 0; t < nSpecs; t++)
            {
                if (specs[t].var)
                    dpiVar_release(specs[t].var);
                if (specs[t].ownedBufs)
                {
                    for (uint32_t r2 = 0; r2 < iters; r2++)
                        if (specs[t].ownedBufs[r2])
                            Tcl_Free(specs[t].ownedBufs[r2]);
                    Tcl_Free((char*)specs[t].ownedBufs);
                }
                if (specs[t].listObj)
                    Tcl_DecrRefCount(specs[t].listObj);
            }
            Tcl_Free((char*)specs);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getInfo");
        }

        dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
        if (info.isDML)
            mode |= DPI_MODE_EXEC_BATCH_ERRORS;
        if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
            mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

        if (dpiStmt_executeMany(s->stmt, mode, iters) != DPI_SUCCESS)
        {
            for (int t = 0; t < nSpecs; t++)
            {
                if (specs[t].var)
                    dpiVar_release(specs[t].var);
                if (specs[t].ownedBufs)
                {
                    for (uint32_t r2 = 0; r2 < iters; r2++)
                        if (specs[t].ownedBufs[r2])
                            Tcl_Free(specs[t].ownedBufs[r2]);
                    Tcl_Free((char*)specs[t].ownedBufs);
                }
                if (specs[t].listObj)
                    Tcl_DecrRefCount(specs[t].listObj);
            }
            Tcl_Free((char*)specs);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_executeMany");
        }

        uint64_t rows = 0;
        if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
            Oradpi_RecordRows((OradpiBase*)s, rows);

        for (int k = 0; k < nSpecs; k++)
        {
            ArrSpec* as = &specs[k];
            if (as->var)
                dpiVar_release(as->var);
            if (as->ownedBufs)
            {
                for (uint32_t r = 0; r < iters; r++)
                    if (as->ownedBufs[r])
                        Tcl_Free(as->ownedBufs[r]);
                Tcl_Free((char*)as->ownedBufs);
            }
            Tcl_DecrRefCount(as->listObj);
        }
        Tcl_Free((char*)specs);

        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    /* Non-arraydml path */
    const char* stmtKey = Tcl_GetString(objv[1]);
    OradpiPendingRefs* pr = GetPendings(ip, stmtKey);
    BindStore* bs = GetBindStore(ip, stmtKey);

    Tcl_Size k = i;
    while (k + 1 < objc && Tcl_GetString(objv[k])[0] == ':')
    {
        const char* nameNoColon = Oradpi_StripColon(Tcl_GetString(objv[k]), NULL);
        Tcl_Obj* val = objv[k + 1];

        if (Oradpi_BindOneByValue(ip, s, pr, nameNoColon, val) != TCL_OK)
        {
            Oradpi_PendingsReleaseAll(pr);
            return TCL_ERROR;
        }

        StoreBind(bs, nameNoColon, val);
        k += 2;
    }

    dpiStmtInfo info;
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS)
    {
        Oradpi_PendingsReleaseAll(pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getInfo");
    }

    dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
    if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
        mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

    uint32_t nqc = 0;
    if (dpiStmt_execute(s->stmt, mode, &nqc) != DPI_SUCCESS)
    {
        Oradpi_PendingsReleaseAll(pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_execute");
    }

    uint64_t rows = 0;
    if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
        Oradpi_RecordRows((OradpiBase*)s, rows);
    Oradpi_UpdateStmtType(s);

    Oradpi_PendingsReleaseAll(pr);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
