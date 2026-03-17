/*
 *  cmd_exec.c --
 *
 *    SQL execution helpers and convenience commands (single-shot and prepared executes).
 *
 *    - Implements Oratcl-compatible autocommit and rows-affected tracking.
 *    - Uses shared bind infrastructure from cmd_bind.c via cmd_int.h.
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

/* ==========================================================================
 * Forward Declarations
 * ========================================================================== */

static int ExecOnce_WithRebind(Tcl_Interp* ip, OradpiStmt* s, const char* skey, int doCommit);

/* ------------------------------------------------------------------------- *
 * Implementation
 * ------------------------------------------------------------------------- */

/* Check whether an ODPI error matches the configured failover error
 * classes.  Returns 1 if the error is retryable, 0 otherwise. */
static int ErrorMatchesFailoverClass(const dpiErrorInfo* ei, uint32_t errClasses)
{
    if (!errClasses)
        return 0;
    if ((errClasses & ORADPI_FO_CLASS_NETWORK) && ei->isRecoverable)
        return 1;
    if (errClasses & ORADPI_FO_CLASS_CONNLOST)
    {
        int c = (int)ei->code;
        if (c == ORA_ERR_BROKEN_PIPE || c == ORA_ERR_NOT_CONNECTED || c == ORA_ERR_LOST_CONTACT || c == ORA_ERR_TNS_LOST_CONTACT)
            return 1;
    }
    return 0;
}

static int ExecOnce_WithRebind(Tcl_Interp* ip, OradpiStmt* s, const char* skey, int doCommit)
{
    if (!s->stmt || !s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is not prepared or connection closed");

    /* If the statement was already executed during oraparse (no-bind query),
     * skip re-execution to avoid a redundant round-trip. Clear the flag so
     * subsequent oraexec calls do execute normally. */
    if (s->executedInParse)
    {
        s->executedInParse = 0;
        Oradpi_UpdateStmtType(s);
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    OradpiPendingRefs pr;
    Oradpi_PendingsInit(&pr);

    if (Oradpi_RebindAllStored(ip, s, &pr, skey) != TCL_OK)
    {
        Oradpi_PendingsFree(&pr);
        return TCL_ERROR;
    }

    dpiStmtInfo info;
    if (dpiStmt_getInfo(s->stmt, &info) != DPI_SUCCESS)
    {
        Oradpi_PendingsFree(&pr);
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s, "dpiStmt_getInfo");
    }

    dpiExecMode mode = DPI_MODE_EXEC_DEFAULT;
    if (doCommit || (s->owner && s->owner->autocommit && (info.isDML || info.isPLSQL)))
        mode |= DPI_MODE_EXEC_COMMIT_ON_SUCCESS;

    /* SUG-4: Execute with retry/backoff when failover policy is configured */
    uint32_t maxAttempts = s->owner ? s->owner->foMaxAttempts : 0;
    uint32_t backoffMs = s->owner ? s->owner->foBackoffMs : 100;
    double backoffFact = s->owner ? s->owner->foBackoffFactor : 2.0;
    uint32_t errClasses = s->owner ? s->owner->foErrorClasses : 0;
    /* V-6 fix: overflow-safe totalTries computation */
    uint32_t totalTries;
    if (maxAttempts > 0 && errClasses)
        totalTries = (maxAttempts >= UINT32_MAX) ? UINT32_MAX : maxAttempts + 1;
    else
        totalTries = 1;

    int execRc = DPI_FAILURE;
    uint32_t nqc = 0;
    dpiErrorInfo lastEi;
    memset(&lastEi, 0, sizeof(lastEi));

    for (uint32_t attempt = 0; attempt < totalTries; attempt++)
    {
        nqc = 0;
        execRc = dpiStmt_execute(s->stmt, mode, &nqc);
        if (execRc == DPI_SUCCESS)
            break;

        /* Capture error immediately (thread-local, must be before next ODPI call) */
        memset(&lastEi, 0, sizeof(lastEi));
        (void)Oradpi_CaptureODPIError(&lastEi);

        if (attempt + 1 < totalTries && ErrorMatchesFailoverClass(&lastEi, errClasses))
        {
            /* Exponential backoff */
            double sleepMs = (double)backoffMs;
            for (uint32_t k = 0; k < attempt; k++)
                sleepMs *= backoffFact;
            if (sleepMs > 60000.0)
                sleepMs = 60000.0; /* cap at 60s */
            /* M-7: Clamp to non-negative (NaN or negative backoffFact) */
            if (!(sleepMs >= 0.0))
                sleepMs = 0.0;
            Tcl_Sleep((int)sleepMs);
            continue;
        }
        break; /* non-retryable error */
    }

    if (execRc != DPI_SUCCESS)
    {
        Oradpi_PendingsFree(&pr);
        /* Use the captured error info to avoid stale thread-local state */
        return Oradpi_SetErrorFromODPIInfo(ip, (OradpiBase*)s, "dpiStmt_execute", &lastEi);
    }

    uint64_t rows = 0;
    if (dpiStmt_getRowCount(s->stmt, &rows) == DPI_SUCCESS)
        Oradpi_RecordRows((OradpiBase*)s, rows);
    Oradpi_UpdateStmtType(s);

    Oradpi_PendingsFree(&pr);
    Oradpi_PendingsForget(ip, skey);

    Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
    return TCL_OK;
}

/*
 * oraexec statement-handle ?-commit?
 *
 *   Executes a previously parsed/bound statement. Rebinds stored bind
 *   variables, supports autocommit and explicit -commit. With failover
 *   policy configured, retries with exponential backoff on matching errors.
 *   Returns: 0 on success.
 *   Errors:  ODPI-C execution errors; invalid/unprepared handle; async busy.
 *   Thread-safety: safe — per-interp state only.
 */
int Oradpi_Cmd_Exec(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2 || objc > 3)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?-commit?");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (Oradpi_StmtIsAsyncBusy(s))
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is busy (async operation in progress)");

    int doCommit = 0;
    if (objc == 3)
    {
        static const char* const execOpts[] = {"-commit", NULL};
        int optIdx = 0;
        if (Tcl_GetIndexFromObj(ip, objv[2], execOpts, "option", 0, &optIdx) != TCL_OK)
            return TCL_ERROR;
        doCommit = 1;
    }

    const char* skey = Tcl_GetString(objv[1]);
    return ExecOnce_WithRebind(ip, s, skey, doCommit);
}

int Oradpi_Cmd_StmtSql(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 3 || objc > 5)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle SQL ?-commit? ?-parseonly?");
        return TCL_ERROR;
    }
    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "connection closed");
    /* V-3 fix: refuse to replace statement while async execution is in flight */
    if (Oradpi_StmtIsAsyncBusy(s))
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is busy (async operation in progress)");

    int doCommit = 0;
    int parseOnly = 0;
    for (Tcl_Size a = 3; a < objc; a++)
    {
        const char* o = Tcl_GetString(objv[a]);
        if (strcmp(o, "-commit") == 0)
            doCommit = 1;
        else if (strcmp(o, "-parseonly") == 0)
            parseOnly = 1;
        else
        {
            Tcl_WrongNumArgs(ip, 1, objv, "statement-handle SQL ?-commit? ?-parseonly?");
            return TCL_ERROR;
        }
    }

    Tcl_Size slen = 0;
    const char* sql = Tcl_GetStringFromObj(objv[2], &slen);
    if (slen < 0 || (uint64_t)slen > UINT32_MAX)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "SQL text exceeds maximum length");
    dpiStmt* newStmt = NULL;
    if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)slen, NULL, 0, &newStmt) != DPI_SUCCESS)
        return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s->owner, "dpiConn_prepareStmt");

    if (s->stmt)
    {
        dpiStmt_close(s->stmt, NULL, 0);
        dpiStmt_release(s->stmt);
    }
    s->stmt = newStmt;

    const char* skey = Tcl_GetString(objv[1]);
    Oradpi_ClearBindStoreForStmt(ip, skey);

    if (parseOnly)
    {
        Oradpi_UpdateStmtType(s);
        Tcl_SetObjResult(ip, Tcl_NewIntObj(0));
        return TCL_OK;
    }

    return ExecOnce_WithRebind(ip, s, skey, doCommit);
}

int Oradpi_Cmd_Plexec(void* cd, Tcl_Interp* ip, Tcl_Size objc, Tcl_Obj* const objv[])
{
    (void)cd;
    if (objc < 2)
    {
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?{PLSQL block}? ?-commit?");
        return TCL_ERROR;
    }
    Tcl_Obj* blockObj = NULL;
    int doCommit = 0;

    Tcl_Size argi = 2;
    while (argi < objc)
    {
        const char* t = Tcl_GetString(objv[argi]);
        if (strcmp(t, "-commit") == 0)
        {
            doCommit = 1;
            argi++;
            continue;
        }
        if (!blockObj)
        {
            blockObj = objv[argi++];
            continue;
        }
        Tcl_WrongNumArgs(ip, 1, objv, "statement-handle ?{PLSQL block}? ?-commit?");
        return TCL_ERROR;
    }

    OradpiStmt* s = Oradpi_LookupStmt(ip, objv[1]);
    if (!s)
        return Oradpi_SetError(ip, NULL, -1, "invalid statement handle");
    if (!s->owner || !s->owner->conn)
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "connection closed");
    /* V-3 fix: refuse to replace statement while async execution is in flight */
    if (Oradpi_StmtIsAsyncBusy(s))
        return Oradpi_SetError(ip, (OradpiBase*)s, -1, "statement is busy (async operation in progress)");

    if (blockObj)
    {
        Tcl_Size bl = 0;
        const char* sql = Tcl_GetStringFromObj(blockObj, &bl);
        if (bl < 0 || (uint64_t)bl > UINT32_MAX)
            return Oradpi_SetError(ip, (OradpiBase*)s, -1, "PL/SQL text exceeds maximum length");
        dpiStmt* newStmt = NULL;
        if (dpiConn_prepareStmt(s->owner->conn, 0, sql, (uint32_t)bl, NULL, 0, &newStmt) != DPI_SUCCESS)
            return Oradpi_SetErrorFromODPI(ip, (OradpiBase*)s->owner, "dpiConn_prepareStmt");
        if (s->stmt)
        {
            dpiStmt_close(s->stmt, NULL, 0);
            dpiStmt_release(s->stmt);
        }
        s->stmt = newStmt;
        Oradpi_ClearBindStoreForStmt(ip, Tcl_GetString(objv[1]));
    }

    const char* skey = Tcl_GetString(objv[1]);
    return ExecOnce_WithRebind(ip, s, skey, doCommit);
}
