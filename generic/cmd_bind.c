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
#include <stdatomic.h>
#include <string.h>
/* strncasecmp is declared in <strings.h> on POSIX, not <string.h> */
#ifndef _WIN32
#include <strings.h>
#endif

#include "cmd_int.h"
#include "dpi.h"

/* BindStoreMap and PendingMap are declared in state.h and embedded in
 * OradpiInterpState.  BindStore remains local to this file. */
typedef struct BindStore {
    Tcl_HashTable byName;
} BindStore;

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static int                          BindOneLobScalar(Tcl_Interp *ip, OradpiStmt *s, OradpiPendingRefs *pr, const char *nameNoColon, dpiOracleTypeNum lobType, const char *buf, uint32_t buflen);
static BindStore                   *GetBindStore(Tcl_Interp *ip, const char *stmtKey);
static BindStoreMap                *GetBindStoreMap(Tcl_Interp *ip);
static PendingMap                  *GetPendingMap(Tcl_Interp *ip);
static OradpiPendingRefs           *GetPendings(Tcl_Interp *ip, const char *stmtKey);
static int                          is_blob_hint(const char *nameNoColon);
static int                          is_clob_hint(const char *nameNoColon);
static void                         StoreBind(BindStore *bs, const char *nameNoColon, Tcl_Obj *v);
static int                          strcasestr_contains(const char *hay, const char *needle);

/* Cached Tcl type pointer to avoid per-bind hash lookup.
 * _Atomic with acquire/release ordering eliminates the data race on the
 * outer read in EnsureBytearrayType's double-check pattern. */
static _Atomic(const Tcl_ObjType *) gBytearrayType = NULL;
/* gTypeInitMutex: protects gBytearrayType publication (one-time init).
 * Lock ordering: leaf lock, no other locks held while this is held. */
static Tcl_Mutex                    gTypeInitMutex;

/* Safely narrow Tcl_Size to uint32_t for ODPI-C APIs; returns TCL_ERROR on overflow */
static int                          CheckU32(Tcl_Interp *ip, Tcl_Size len, uint32_t *out) {
    if (len < 0 || (uint64_t)len > UINT32_MAX) {
        Tcl_SetObjResult(ip, Tcl_NewStringObj("value length exceeds uint32_t range for ODPI-C", -1));
        return TCL_ERROR;
    }
    *out = (uint32_t)len;
    return TCL_OK;
}

static const Tcl_ObjType *EnsureBytearrayType(void) {
    /* Fast path: acquire-load the cached pointer without taking the lock.
     * The release-store in the slow path ensures the resolved pointer is
     * fully visible to all threads after this load. */
    const Tcl_ObjType *cached = atomic_load_explicit(&gBytearrayType, memory_order_acquire);
    if (cached)
        return cached;

    /* Slow path: resolve the type outside the lock (Tcl API calls must
     * not be made while holding gTypeInitMutex — deadlock risk). */
    const Tcl_ObjType *resolved = Tcl_GetObjType("bytearray");
    if (!resolved) {
        /* Fallback: force Tcl to register the type by creating a temp obj */
        Tcl_Obj *tmp = Tcl_NewByteArrayObj(NULL, 0);
        Tcl_IncrRefCount(tmp);
        resolved = Tcl_GetObjType("bytearray");
        Tcl_DecrRefCount(tmp);
    }

    /* Publish result under lock with release ordering; if another thread
     * raced us, the value is the same (Tcl_ObjType* is process-global). */
    Tcl_MutexLock(&gTypeInitMutex);
    if (!atomic_load_explicit(&gBytearrayType, memory_order_relaxed))
        atomic_store_explicit(&gBytearrayType, resolved, memory_order_release);
    cached = atomic_load_explicit(&gBytearrayType, memory_order_relaxed);
    Tcl_MutexUnlock(&gTypeInitMutex);
    return cached;
}

/* Non-shimmering check for bytearray internal rep via public API.
 * Avoids direct access to Tcl_Obj.typePtr which is private in Tcl 9. */
static int IsBytearrayObj(Tcl_Obj *obj) {
    const Tcl_ObjType *baType = EnsureBytearrayType();
    if (!baType)
        return 0;
    return (Tcl_FetchInternalRep(obj, baType) != NULL);
}

#ifdef _WIN32
#include <ctype.h>
static char *strcasestr(const char *haystack, const char *needle) {
    if (!haystack || !needle)
        return NULL;
    size_t needle_len = strlen(needle);
    if (needle_len == 0)
        return (char *)haystack;
    for (; *haystack; haystack++) {
        if (_strnicmp(haystack, needle, needle_len) == 0)
            return (char *)haystack;
    }
    return NULL;
}
#endif

/* ------------------------------------------------------------------------- *
 * Shared utility functions (exported via cmd_int.h)
 * ------------------------------------------------------------------------- */

uint32_t Oradpi_WithColon(const char *nameNoColon, char *dst, uint32_t cap) {
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

const char *Oradpi_StripColon(const char *raw) {
    const char *p = raw ? raw : "";
    if (*p == ':')
        p++;
    return p;
}

/* ---- Bind name resolution (consistent order: colon-prefixed first) ---- */

static int BindVarByNameDual(OradpiStmt *s, const char *nameNoColon, dpiVar *var, Tcl_Interp *ip, const char *ctx) {
    char     buf[256];
    uint32_t m = Oradpi_WithColon(nameNoColon, buf, (uint32_t)sizeof(buf));
    CONN_GATE_ENTER(s->owner);
    if (m && dpiStmt_bindByName(s->stmt, buf, m, var) == DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        return TCL_OK;
    }

    /* m = strlen(nameNoColon)+1 when the colon-prefixed name fit in buf.
     * Reuse it to derive the bare-name length instead of calling strlen
     * a second time.  Only fall back to strlen when m==0 (name exceeded
     * buf[256] capacity — not a normal Oracle identifier). */
    uint32_t nlen;
    if (m > 0) {
        nlen = m - 1;
    } else {
        Tcl_Size rawLen = (Tcl_Size)strlen(nameNoColon);
        if (CheckU32(ip, rawLen, &nlen) != TCL_OK) {
            CONN_GATE_LEAVE(s->owner);
            return TCL_ERROR;
        }
    }
    if (dpiStmt_bindByName(s->stmt, nameNoColon, nlen, var) == DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        return TCL_OK;
    }

    CONN_GATE_LEAVE(s->owner);
    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, ctx);
}

static int BindValueByNameDual(OradpiStmt *s, const char *nameNoColon, dpiNativeTypeNum ntn, dpiData *d, Tcl_Interp *ip, const char *ctx) {
    char     buf[256];
    uint32_t m = Oradpi_WithColon(nameNoColon, buf, (uint32_t)sizeof(buf));
    CONN_GATE_ENTER(s->owner);
    if (m && dpiStmt_bindValueByName(s->stmt, buf, m, ntn, d) == DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        return TCL_OK;
    }

    /* m = strlen(nameNoColon)+1 when the colon-prefixed name fit in buf.
     * Reuse it to derive the bare-name length instead of calling strlen
     * a second time.  Only fall back to strlen when m==0 (name exceeded
     * buf[256] capacity — not a normal Oracle identifier). */
    uint32_t nlen;
    if (m > 0) {
        nlen = m - 1;
    } else {
        Tcl_Size rawLen = (Tcl_Size)strlen(nameNoColon);
        if (CheckU32(ip, rawLen, &nlen) != TCL_OK) {
            CONN_GATE_LEAVE(s->owner);
            return TCL_ERROR;
        }
    }
    if (dpiStmt_bindValueByName(s->stmt, nameNoColon, nlen, ntn, d) == DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        return TCL_OK;
    }

    CONN_GATE_LEAVE(s->owner);
    return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, ctx);
}

/* ---- Hint helpers ---- */

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

/* ---- BindStore (per-statement stored binds) ---- */

/* BindStoreMap is embedded in OradpiInterpState, so this
 * function clears the map contents without freeing the map struct itself.
 * Called from Oradpi_DeleteInterpData (state.c) during Phase 1.5 teardown.
 *
 * IMPORTANT: Tcl_DeleteHashTable frees all internal storage and leaves the
 * table in an uninitialised state.  Phase 3 (FreeStmt) calls
 * Oradpi_BindStoreForget, which calls Tcl_FindHashEntry on this same table.
 * To avoid a use-after-delete crash we must re-initialise the table to a
 * valid empty state immediately after deleting it. */
void Oradpi_ClearBindStoreMap(BindStoreMap *bm) {
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
    /* Re-initialise so that subsequent Tcl_FindHashEntry calls from Phase 3
     * FreeStmt → Oradpi_BindStoreForget operate on a valid empty table. */
    Tcl_InitHashTable(&bm->byStmt, TCL_STRING_KEYS);
}

static BindStoreMap *GetBindStoreMap(Tcl_Interp *ip) {
    /* use the embedded map instead of a separate AssocData entry */
    OradpiInterpState *st = Oradpi_GetInterpState(ip);
    return &st->bindStoreMap;
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
    /* embedded map — no AssocData lookup needed */
    OradpiInterpState *st = Oradpi_GetInterpState(ip);
    BindStoreMap      *bm = &st->bindStoreMap;
    Tcl_HashEntry     *he = Tcl_FindHashEntry(&bm->byStmt, stmtKey);
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

void Oradpi_ClearBindStoreForStmt(Tcl_Interp *ip, const char *stmtKey) {
    Oradpi_BindStoreForget(ip, stmtKey);
}

/* ---- PendingRefs (dpiVar* refs kept alive until execution) ---- */

/* PendingMap is embedded in OradpiInterpState; this
 * function clears its contents without freeing the map struct itself.
 * Called from Oradpi_DeleteInterpData (state.c) during Phase 1.5 teardown.
 *
 * IMPORTANT: same re-init requirement as Oradpi_ClearBindStoreMap — Phase 3
 * FreeStmt calls Oradpi_PendingsForget which calls Tcl_FindHashEntry on this
 * table.  Re-initialise to a valid empty state after deletion. */
void Oradpi_ClearPendingMap(PendingMap *pm) {
    if (!pm)
        return;
    Tcl_HashSearch hs;
    for (Tcl_HashEntry *e = Tcl_FirstHashEntry(&pm->byStmt, &hs); e; e = Tcl_NextHashEntry(&hs)) {
        OradpiPendingRefs *pr = (OradpiPendingRefs *)Tcl_GetHashValue(e);
        if (!pr)
            continue;
        for (Tcl_Size i = 0; i < pr->n; i++)
            if (pr->vars[i])
                dpiVar_release(pr->vars[i]);
        if (pr->vars)
            Tcl_Free((char *)pr->vars);
        Tcl_Free((char *)pr);
    }
    Tcl_DeleteHashTable(&pm->byStmt);
    /* Re-initialise so that subsequent Tcl_FindHashEntry calls from Phase 3
     * FreeStmt → Oradpi_PendingsForget operate on a valid empty table. */
    Tcl_InitHashTable(&pm->byStmt, TCL_STRING_KEYS);
}

static PendingMap *GetPendingMap(Tcl_Interp *ip) {
    /* use the embedded map instead of a separate AssocData entry */
    OradpiInterpState *st = Oradpi_GetInterpState(ip);
    return &st->pendingMap;
}

static OradpiPendingRefs *GetPendings(Tcl_Interp *ip, const char *stmtKey) {
    PendingMap        *pm    = GetPendingMap(ip);
    int                isNew = 0;
    Tcl_HashEntry     *he    = Tcl_CreateHashEntry(&pm->byStmt, stmtKey, &isNew);
    OradpiPendingRefs *pr    = isNew ? NULL : (OradpiPendingRefs *)Tcl_GetHashValue(he);
    if (!pr) {
        size_t varBytes = 0;
        pr              = (OradpiPendingRefs *)Tcl_Alloc(sizeof(*pr));
        pr->n           = 0;
        pr->cap         = 4;
        if (Oradpi_CheckedAllocBytes(NULL, pr->cap, sizeof(dpiVar *), &varBytes, "pending var table") != TCL_OK) {
            pr->vars = NULL;
            pr->cap  = 0;
        } else
            pr->vars = (dpiVar **)Tcl_Alloc(varBytes);
        Tcl_SetHashValue(he, pr);
    }
    return pr;
}

/* Public PendingRefs management for stack-local use (cmd_exec.c) */
void Oradpi_PendingsInit(OradpiPendingRefs *pr) {
    size_t varBytes = 0;
    pr->n           = 0;
    pr->cap         = 8;
    if (Oradpi_CheckedAllocBytes(NULL, pr->cap, sizeof(dpiVar *), &varBytes, "pending var table") != TCL_OK) {
        pr->vars = NULL;
        pr->cap  = 0;
        return;
    }
    pr->vars = (dpiVar **)Tcl_Alloc(varBytes);
}

void Oradpi_PendingsAdd(OradpiPendingRefs *pr, dpiVar *v) {
    if (!pr || !pr->vars || pr->cap <= 0)
        return;
    if (pr->n == pr->cap) {
        Tcl_Size newCap   = 0;
        size_t   varBytes = 0;
        if (pr->cap > TCL_SIZE_MAX / 2)
            return;
        newCap = pr->cap * 2;
        if (Oradpi_CheckedAllocBytes(NULL, newCap, sizeof(dpiVar *), &varBytes, "pending var table") != TCL_OK)
            return;
        pr->vars = (dpiVar **)Tcl_Realloc((char *)pr->vars, varBytes);
        memset(pr->vars + pr->cap, 0, sizeof(dpiVar *) * (size_t)(newCap - pr->cap));
        pr->cap = newCap;
    }
    pr->vars[pr->n++] = v;
}

void Oradpi_PendingsReleaseAll(OradpiPendingRefs *pr) {
    for (Tcl_Size i = 0; i < pr->n; i++)
        if (pr->vars[i])
            dpiVar_release(pr->vars[i]);
    pr->n = 0;
}

void Oradpi_PendingsFree(OradpiPendingRefs *pr) {
    Oradpi_PendingsReleaseAll(pr);
    if (pr->vars) {
        Tcl_Free((char *)pr->vars);
        pr->vars = NULL;
    }
    pr->cap = 0;
}

void Oradpi_PendingsForget(Tcl_Interp *ip, const char *stmtKey) {
    /* embedded map — no AssocData lookup needed */
    OradpiInterpState *st = Oradpi_GetInterpState(ip);
    PendingMap        *pm = &st->pendingMap;
    Tcl_HashEntry     *he = Tcl_FindHashEntry(&pm->byStmt, stmtKey);
    if (!he)
        return;
    OradpiPendingRefs *pr = (OradpiPendingRefs *)Tcl_GetHashValue(he);
    if (pr) {
        for (Tcl_Size i = 0; i < pr->n; i++)
            if (pr->vars[i])
                dpiVar_release(pr->vars[i]);
        if (pr->vars)
            Tcl_Free((char *)pr->vars);
        Tcl_Free((char *)pr);
    }
    Tcl_DeleteHashEntry(he);
}

/* ---- Core bind-by-value logic (shared) ---- */

static int BindOneLobScalar(Tcl_Interp *ip, OradpiStmt *s, OradpiPendingRefs *pr, const char *nameNoColon, dpiOracleTypeNum lobType, const char *buf, uint32_t buflen) {
    dpiVar  *var  = NULL;
    dpiData *data = NULL;
    dpiLob  *lob  = NULL;

    CONN_GATE_ENTER(s->owner);
    if (dpiConn_newVar(s->owner->conn, lobType, DPI_NATIVE_TYPE_LOB, 1, 0, 0, 0, NULL, &var, &data) != DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_newVar(LOB)");
    }

    if (dpiConn_newTempLob(s->owner->conn, lobType, &lob) != DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        dpiVar_release(var);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_newTempLob");
    }
    if (buflen > 0) {
        if (dpiLob_setFromBytes(lob, buf, (uint64_t)buflen) != DPI_SUCCESS) {
            CONN_GATE_LEAVE(s->owner);
            dpiLob_release(lob);
            dpiVar_release(var);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiLob_setFromBytes");
        }
    }
    if (dpiVar_setFromLob(var, 0, lob) != DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        dpiLob_release(lob);
        dpiVar_release(var);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiVar_setFromLob");
    }
    dpiLob_release(lob);
    CONN_GATE_LEAVE(s->owner);

    if (BindVarByNameDual(s, nameNoColon, var, ip, "dpiStmt_bindByName(LOB)") != TCL_OK) {
        dpiVar_release(var);
        return TCL_ERROR;
    }

    Oradpi_PendingsAdd(pr, var);
    return TCL_OK;
}

int Oradpi_BindOneByValue(Tcl_Interp *ip, OradpiStmt *s, OradpiPendingRefs *pr, const char *nameNoColon, Tcl_Obj *valueObj) {
    if (is_blob_hint(nameNoColon)) {
        Tcl_Size             blen   = 0;
        const unsigned char *bp     = NULL;
        uint32_t             blen32 = 0;
        if (IsBytearrayObj(valueObj)) {
            bp = Tcl_GetByteArrayFromObj(valueObj, &blen);
            if (CheckU32(ip, blen, &blen32) != TCL_OK)
                return TCL_ERROR;
            return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char *)bp, blen32);
        } else {
            const char *sv = Tcl_GetStringFromObj(valueObj, &blen);
            if (CheckU32(ip, blen, &blen32) != TCL_OK)
                return TCL_ERROR;
            return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, blen32);
        }
    }

    if (IsBytearrayObj(valueObj)) {
        Tcl_Size       blen   = 0;
        uint32_t       blen32 = 0;
        unsigned char *b      = Tcl_GetByteArrayFromObj(valueObj, &blen);
        if (CheckU32(ip, blen, &blen32) != TCL_OK)
            return TCL_ERROR;
        return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, (const char *)b, blen32);
    }

    Tcl_Size    sl = 0;
    const char *sv = Tcl_GetStringFromObj(valueObj, &sl);

    if (sl > 0 && memchr(sv, '\0', (size_t)sl) != NULL) {
        uint32_t sl32 = 0;
        if (CheckU32(ip, sl, &sl32) != TCL_OK)
            return TCL_ERROR;
        return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_BLOB, sv, sl32);
    }

    if (is_clob_hint(nameNoColon) && sl > 0) {
        if (sl > 4000) {
            uint32_t sl32 = 0;
            if (CheckU32(ip, sl, &sl32) != TCL_OK)
                return TCL_ERROR;
            return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, sl32);
        }
    }

    if (sl > 4000) {
        uint32_t sl32 = 0;
        if (CheckU32(ip, sl, &sl32) != TCL_OK)
            return TCL_ERROR;
        return BindOneLobScalar(ip, s, pr, nameNoColon, DPI_ORACLE_TYPE_CLOB, sv, sl32);
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
    /* Use cached encoding from connection to avoid per-bind ODPI call */
    enc.encoding = s->owner->cachedEncoding;
    if (!enc.encoding) {
        /* Fallback if cache not populated (e.g., adopted connection) */
        CONN_GATE_ENTER(s->owner);
        (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);
        CONN_GATE_LEAVE(s->owner);
    }
    uint32_t sl32 = 0;
    if (CheckU32(ip, sl, &sl32) != TCL_OK)
        return TCL_ERROR;
    d.value.asBytes.ptr      = (char *)sv;
    d.value.asBytes.length   = sl32;
    d.value.asBytes.encoding = enc.encoding;
    return BindValueByNameDual(s, nameNoColon, DPI_NATIVE_TYPE_BYTES, &d, ip, "dpiStmt_bindValueByName(bytes)");
}

/* Rebind all stored binds for a statement (used by cmd_exec.c) */
int Oradpi_RebindAllStored(Tcl_Interp *ip, OradpiStmt *s, OradpiPendingRefs *pr, const char *stmtKey) {
    /* use embedded map via Oradpi_GetInterpState, not AssocData */
    OradpiInterpState *st = Oradpi_GetInterpState(ip);
    BindStoreMap      *bm = &st->bindStoreMap;
    Tcl_HashEntry     *he = Tcl_FindHashEntry(&bm->byStmt, stmtKey);
    if (!he)
        return TCL_OK;
    BindStore *bs = (BindStore *)Tcl_GetHashValue(he);
    if (!bs)
        return TCL_OK;
    Tcl_HashSearch hs;
    for (Tcl_HashEntry *e = Tcl_FirstHashEntry(&bs->byName, &hs); e; e = Tcl_NextHashEntry(&hs)) {
        const char *nameNoColon = (const char *)Tcl_GetHashKey(&bs->byName, e);
        Tcl_Obj    *val         = (Tcl_Obj *)Tcl_GetHashValue(e);
        if (!val)
            continue;
        if (Oradpi_BindOneByValue(ip, s, pr, nameNoColon, val) != TCL_OK)
            return TCL_ERROR;
    }
    return TCL_OK;
}

/* ---- Command implementations ---- */

/*
 * orabind statement-handle :name value ?:name value ...?
 *
 *   Binds one or more named parameters to a prepared statement by value.
 *   Bind names must start with ':'. Values are stored for automatic rebind
 *   on subsequent oraexec calls. Type inference: int64 > double > string;
 *   bytearray → BLOB; name hinting (blob/clob) overrides type inference.
 *   Returns: 0 on success.
 *   Errors:  ODPI-C bind errors; invalid/unprepared handle; missing pairs.
 *   Thread-safety: safe — per-interp bind store only.
 */
int Oradpi_Cmd_Orabind(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 3) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle :name value ? :name value ... ?");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "statement is not prepared or connection closed");
    /* refuse to bind while async execution is in flight */
    if (Oradpi_StmtIsAsyncBusy(s))
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "statement is busy (async operation in progress)");

    const char        *stmtKey = Tcl_GetString(objv[1]);
    OradpiPendingRefs *pr      = GetPendings(ip, stmtKey);
    Oradpi_PendingsReleaseAll(pr);
    BindStore *bs  = GetBindStore(ip, stmtKey);

    Tcl_Size   i   = 2;
    int        saw = 0;
    while (i + 1 < objc && Tcl_GetString(objv[i])[0] == ':') {
        const char *nameNoColon = Oradpi_StripColon(Tcl_GetString(objv[i]));
        Tcl_Obj    *val         = objv[i + 1];

        if (Oradpi_BindOneByValue(ip, s, pr, nameNoColon, val) != TCL_OK)
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

/* Extracted arraydml spec struct and cleanup helper to eliminate
 * 6+ repeated cleanup blocks in the arraydml path. */
typedef struct ArrSpec {
    const char      *nameNoColon;
    Tcl_Obj         *listObj;
    Tcl_Size         count;
    dpiOracleTypeNum ora;
    dpiNativeTypeNum nat;
    uint32_t         elemSize;
    int              sizeIsBytes;
    dpiVar          *var;
    dpiData         *data;
    char           **ownedBufs;
} ArrSpec;

/* Free all resources held by an array of ArrSpec entries.
 * nSpecs: number of valid entries; iters: row count for ownedBufs. */
static void FreeArrSpecs(ArrSpec *specs, Tcl_Size nSpecs, uint32_t iters) {
    for (Tcl_Size t = 0; t < nSpecs; t++) {
        if (specs[t].var)
            dpiVar_release(specs[t].var);
        if (specs[t].ownedBufs) {
            for (uint32_t r = 0; r < iters; r++)
                if (specs[t].ownedBufs[r])
                    Tcl_Free(specs[t].ownedBufs[r]);
            Tcl_Free((char *)specs[t].ownedBufs);
        }
        if (specs[t].listObj)
            Tcl_DecrRefCount(specs[t].listObj);
    }
    Tcl_Free((char *)specs);
}

/*
 * orabindexec statement-handle ?-commit? ?-arraydml? :name value|list ...
 *
 *   Binds and executes in one call. With -arraydml, accepts lists of equal
 *   length for batch DML via dpiStmt_executeMany. Without -arraydml, binds
 *   scalar values and executes a single row. Supports autocommit.
 *   Returns: 0 on success.
 *   Errors:  ODPI-C bind/exec errors; list length mismatch (-arraydml).
 *   Thread-safety: safe — per-interp state only.
 */
int Oradpi_Cmd_Orabindexec(void *cd, Tcl_Interp *ip, Tcl_Size objc, Tcl_Obj *const objv[]) {
    (void)cd;
    if (objc < 2) {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit? ?-arraydml? :name value|list ...");
        return TCL_ERROR;
    }

    OradpiStmt *s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "statement is not prepared or connection closed");
    /* refuse to bind+exec while async execution is in flight */
    if (Oradpi_StmtIsAsyncBusy(s))
        return Oradpi_SetError(ip, (OradpiBase *)s, -1, "statement is busy (async operation in progress)");

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
        /* FreeArrSpecs helper (defined above) replaces 6+ inline cleanup blocks */
        size_t   specsBytes = 0;
        Tcl_Size cap = 8, nSpecs = 0;
        if (Oradpi_CheckedAllocBytes(ip, cap, sizeof(ArrSpec), &specsBytes, "array DML spec table") != TCL_OK)
            return TCL_ERROR;
        ArrSpec *specs    = (ArrSpec *)Tcl_Alloc(specsBytes);
        Tcl_Size expected = -1;

        Tcl_Size j        = i;
        while (j + 1 < objc && Tcl_GetString(objv[j])[0] == ':') {
            if (nSpecs == cap) {
                Tcl_Size newCap     = 0;
                size_t   specsBytes = 0;
                if (cap > TCL_SIZE_MAX / 2) {
                    FreeArrSpecs(specs, nSpecs, 0);
                    return Oradpi_SetError(ip, (OradpiBase *)s, -1, "array DML spec table is too large");
                }
                newCap = cap * 2;
                if (Oradpi_CheckedAllocBytes(ip, newCap, sizeof(ArrSpec), &specsBytes, "array DML spec table") != TCL_OK) {
                    FreeArrSpecs(specs, nSpecs, 0);
                    return TCL_ERROR;
                }
                specs = (ArrSpec *)Tcl_Realloc((char *)specs, specsBytes);
                cap   = newCap;
            }
            ArrSpec *as = &specs[nSpecs];
            memset(as, 0, sizeof(*as));

            as->nameNoColon = Oradpi_StripColon(Tcl_GetString(objv[j]));
            as->listObj     = objv[j + 1];
            Tcl_IncrRefCount(as->listObj);

            if (Tcl_ListObjLength(ip, as->listObj, &as->count) != TCL_OK) {
                FreeArrSpecs(specs, nSpecs + 1, 0);
                return TCL_ERROR;
            }

            if (expected < 0)
                expected = as->count;
            if (as->count != expected) {
                Tcl_Obj *msg = Tcl_NewStringObj("-arraydml list lengths mismatch: :", -1);
                Tcl_AppendToObj(msg, as->nameNoColon, -1);
                Tcl_AppendPrintfToObj(msg, " has %" TCL_SIZE_MODIFIER "d vs expected %" TCL_SIZE_MODIFIER "d", as->count, expected);
                FreeArrSpecs(specs, nSpecs + 1, 0);
                Tcl_SetObjResult(ip, msg);
                return TCL_ERROR;
            }

            as->ora         = DPI_ORACLE_TYPE_VARCHAR;
            as->nat         = DPI_NATIVE_TYPE_BYTES;
            as->elemSize    = 1;
            as->sizeIsBytes = 1;
            /* Scan ALL elements to determine type consistently.
             * Only use numeric type if every element parses as that type. */
            int allInt = 1, allNumeric = 1;
            for (Tcl_Size p = 0; p < as->count; p++) {
                Tcl_Obj *ep = NULL;
                Tcl_ListObjIndex(ip, as->listObj, p, &ep);
                Tcl_IncrRefCount(ep);
                Tcl_WideInt wi;
                double      dd;
                if (Tcl_GetWideIntFromObj(NULL, ep, &wi) != TCL_OK)
                    allInt = 0;
                if (allInt == 0 && Tcl_GetDoubleFromObj(NULL, ep, &dd) != TCL_OK)
                    allNumeric = 0;
                /* use CheckU32 instead of raw (uint32_t) cast */
                Tcl_Size sl = 0;
                (void)Tcl_GetStringFromObj(ep, &sl);
                uint32_t sl32 = 0;
                if (CheckU32(ip, sl, &sl32) != TCL_OK) {
                    Tcl_DecrRefCount(ep);
                    FreeArrSpecs(specs, nSpecs + 1, 0);
                    return TCL_ERROR;
                }
                if (sl32 > as->elemSize)
                    as->elemSize = sl32;
                Tcl_DecrRefCount(ep);
            }
            if (allInt) {
                as->ora = DPI_ORACLE_TYPE_NUMBER;
                as->nat = DPI_NATIVE_TYPE_INT64;
            } else if (allNumeric) {
                as->ora = DPI_ORACLE_TYPE_NUMBER;
                as->nat = DPI_NATIVE_TYPE_DOUBLE;
            }
            if (as->nat == DPI_NATIVE_TYPE_BYTES && as->elemSize == 0)
                as->elemSize = 1;

            nSpecs++;
            j += 2;
        }

        if (nSpecs == 0) {
            FreeArrSpecs(specs, 0, 0);
            Tcl_SetObjResult(ip, Tcl_NewStringObj("orabindexec -arraydml requires :name list pairs", -1));
            return TCL_ERROR;
        }

        if (expected < 0 || (uint64_t)expected > UINT32_MAX) {
            FreeArrSpecs(specs, nSpecs, 0);
            return Oradpi_SetError(ip, (OradpiBase *)s, -1, "-arraydml row count exceeds ODPI-C uint32_t range");
        }

        uint32_t        iters = (uint32_t)expected;

        dpiEncodingInfo enc;
        memset(&enc, 0, sizeof enc);
        /* Use cached encoding */
        enc.encoding = s->owner->cachedEncoding;
        if (!enc.encoding) {
            CONN_GATE_ENTER(s->owner);
            (void)dpiConn_getEncodingInfo(s->owner->conn, &enc);
            CONN_GATE_LEAVE(s->owner);
        }

        for (Tcl_Size k = 0; k < nSpecs; k++) {
            ArrSpec *as = &specs[k];

            CONN_GATE_ENTER(s->owner);
            if (dpiConn_newVar(s->owner->conn, as->ora, as->nat, iters, (as->nat == DPI_NATIVE_TYPE_BYTES) ? as->elemSize : 0, (as->nat == DPI_NATIVE_TYPE_BYTES), 0, NULL, &as->var, &as->data) !=
                DPI_SUCCESS) {
                CONN_GATE_LEAVE(s->owner);
                FreeArrSpecs(specs, nSpecs, iters);
                return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiConn_newVar(array)");
            }
            CONN_GATE_LEAVE(s->owner);

            if (as->nat == DPI_NATIVE_TYPE_BYTES) {
                size_t ownedBufBytes = 0;
                if (Oradpi_CheckedAllocBytes(ip, (Tcl_Size)iters, sizeof(char *), &ownedBufBytes, "array DML buffer table") != TCL_OK) {
                    FreeArrSpecs(specs, nSpecs, iters);
                    return TCL_ERROR;
                }
                as->ownedBufs = (char **)Tcl_Alloc(ownedBufBytes);
                memset(as->ownedBufs, 0, ownedBufBytes);
            } else {
                as->ownedBufs = NULL;
            }

            for (uint32_t r = 0; r < iters; r++) {
                Tcl_Obj *e = NULL;
                Tcl_ListObjIndex(ip, as->listObj, (Tcl_Size)r, &e);
                Tcl_IncrRefCount(e);
                if (as->nat == DPI_NATIVE_TYPE_INT64) {
                    Tcl_WideInt wi;
                    /* check return code — element may have been mutated
                     * or may overflow Tcl_WideInt range since the pre-scan. */
                    if (Tcl_GetWideIntFromObj(NULL, e, &wi) != TCL_OK) {
                        Tcl_DecrRefCount(e);
                        Tcl_SetObjResult(ip, Tcl_ObjPrintf("orabindexec -arraydml: element %u is not a valid integer", r));
                        FreeArrSpecs(specs, nSpecs, iters);
                        return TCL_ERROR;
                    }
                    as->data[r].isNull        = 0;
                    as->data[r].value.asInt64 = (int64_t)wi;
                } else if (as->nat == DPI_NATIVE_TYPE_DOUBLE) {
                    double dd;
                    /* check return code */
                    if (Tcl_GetDoubleFromObj(NULL, e, &dd) != TCL_OK) {
                        Tcl_DecrRefCount(e);
                        Tcl_SetObjResult(ip, Tcl_ObjPrintf("orabindexec -arraydml: element %u is not a valid number", r));
                        FreeArrSpecs(specs, nSpecs, iters);
                        return TCL_ERROR;
                    }
                    as->data[r].isNull         = 0;
                    as->data[r].value.asDouble = dd;
                } else {
                    Tcl_Size    sl   = 0;
                    const char *sv   = Tcl_GetStringFromObj(e, &sl);
                    /* guard narrowing to uint32_t */
                    uint32_t    sl32 = 0;
                    if (CheckU32(ip, sl, &sl32) != TCL_OK) {
                        Tcl_DecrRefCount(e);
                        FreeArrSpecs(specs, nSpecs, iters);
                        return TCL_ERROR;
                    }
                    char *buf = NULL;
                    if (sl > 0) {
                        size_t elemBytes = 0;
                        if (Oradpi_CheckedTclSizeToSizeT(ip, sl, &elemBytes, "array DML element size") != TCL_OK) {
                            Tcl_DecrRefCount(e);
                            FreeArrSpecs(specs, nSpecs, iters);
                            return TCL_ERROR;
                        }
                        buf = (char *)Tcl_Alloc(elemBytes);
                        memcpy(buf, sv, elemBytes);
                    } else {
                        buf = (char *)Tcl_Alloc(1);
                    }
                    as->ownedBufs[r]                   = buf;
                    as->data[r].isNull                 = 0;
                    as->data[r].value.asBytes.ptr      = buf;
                    as->data[r].value.asBytes.length   = sl32;
                    as->data[r].value.asBytes.encoding = enc.encoding;
                }
                Tcl_DecrRefCount(e);
            }

            if (BindVarByNameDual(s, as->nameNoColon, as->var, ip, "dpiStmt_bindByName(array)") != TCL_OK) {
                FreeArrSpecs(specs, nSpecs, iters);
                return TCL_ERROR;
            }
        }

        dpiStmtInfo info;
        CONN_GATE_ENTER(s->owner);
        if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
            CONN_GATE_LEAVE(s->owner);
            FreeArrSpecs(specs, nSpecs, iters);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getInfo");
        }
        CONN_GATE_LEAVE(s->owner);

        dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
        if (info.isDML)
            mode |= DPI_MODE_EXEC_BATCH_ERRORS;
        if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
            mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

        CONN_GATE_ENTER(s->owner);
        if (dpiStmt_executeMany(s->stmt, mode, iters) != DPI_SUCCESS) {
            CONN_GATE_LEAVE(s->owner);
            FreeArrSpecs(specs, nSpecs, iters);
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_executeMany");
        }

        /* I5: when DPI_MODE_EXEC_BATCH_ERRORS is set, individual row failures
         * are collected rather than aborting the whole batch.  Inspect the
         * error array and report any failures back to the caller as a Tcl
         * list of {rowOffset oraCode message} triples. */
        if (mode & DPI_MODE_EXEC_BATCH_ERRORS) {
            uint32_t errCount = 0;
            if (dpiStmt_getBatchErrorCount(s->stmt, &errCount) == DPI_SUCCESS && errCount > 0) {
                dpiErrorInfo *errs     = (dpiErrorInfo *)Tcl_Alloc(errCount * sizeof(dpiErrorInfo));
                int           reported = (dpiStmt_getBatchErrors(s->stmt, errCount, errs) == DPI_SUCCESS);
                if (reported) {
                    Tcl_Obj *errList = Tcl_NewListObj(0, NULL);
                    for (uint32_t e = 0; e < errCount; e++) {
                        Tcl_Obj *triple = Tcl_NewListObj(0, NULL);
                        Tcl_ListObjAppendElement(ip, triple, Tcl_NewWideIntObj((Tcl_WideInt)errs[e].offset));
                        Tcl_ListObjAppendElement(ip, triple, Tcl_NewWideIntObj((Tcl_WideInt)errs[e].code));
                        Tcl_ListObjAppendElement(ip, triple, Tcl_NewStringObj(errs[e].message ? errs[e].message : "", -1));
                        Tcl_ListObjAppendElement(ip, errList, triple);
                    }
                    Tcl_Free((char *)errs);
                    CONN_GATE_LEAVE(s->owner);
                    FreeArrSpecs(specs, nSpecs, iters);
                    /* Store in oramsg for introspection, and return error */
                    Tcl_SetObjResult(ip, errList);
                    Tcl_SetErrorCode(ip, "ORATCL", "BATCH", NULL);
                    return TCL_ERROR;
                }
                Tcl_Free((char *)errs);
            }
        }

        uint64_t rows = 0;
        if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
            Oradpi_RecordRows((OradpiBase *)s, rows);
        CONN_GATE_LEAVE(s->owner);

        FreeArrSpecs(specs, nSpecs, iters);

        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    /* Non-arraydml path */
    const char        *stmtKey = Tcl_GetString(objv[1]);
    OradpiPendingRefs *pr      = GetPendings(ip, stmtKey);
    BindStore         *bs      = GetBindStore(ip, stmtKey);

    Tcl_Size           k       = i;
    while (k + 1 < objc && Tcl_GetString(objv[k])[0] == ':') {
        const char *nameNoColon = Oradpi_StripColon(Tcl_GetString(objv[k]));
        Tcl_Obj    *val         = objv[k + 1];

        if (Oradpi_BindOneByValue(ip, s, pr, nameNoColon, val) != TCL_OK) {
            Oradpi_PendingsReleaseAll(pr);
            return TCL_ERROR;
        }

        StoreBind(bs, nameNoColon, val);
        k += 2;
    }

    dpiStmtInfo info;
    CONN_GATE_ENTER(s->owner);
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        Oradpi_PendingsReleaseAll(pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_getInfo");
    }
    CONN_GATE_LEAVE(s->owner);

    dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
    if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
        mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

    uint32_t nqc = 0;
    CONN_GATE_ENTER(s->owner);
    if (dpiStmt_execute(s->stmt, mode, &nqc) != DPI_SUCCESS) {
        CONN_GATE_LEAVE(s->owner);
        Oradpi_PendingsReleaseAll(pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase *)s, "dpiStmt_execute");
    }

    uint64_t rows = 0;
    if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
        Oradpi_RecordRows((OradpiBase *)s, rows);
    CONN_GATE_LEAVE(s->owner);
    Oradpi_UpdateStmtType(s);

    Oradpi_PendingsReleaseAll(pr);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}
