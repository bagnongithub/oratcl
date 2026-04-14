#!/usr/bin/env tclsh
# =============================================================================
# oratcl_bench.tcl — OratCL performance benchmark
#
# Measures orafetch and orabind throughput across several access patterns.
# Run BEFORE and AFTER applying the performance patches, save both outputs,
# and compare them.  Synthetic rows are generated from DUAL CONNECT BY LEVEL,
# so no pre-existing tables are needed for fetch tests.
#
# Requirements:  Oracle 12c+,  oratcl package loadable from tclsh.
#
# Usage
#   tclsh oratcl_bench.tcl
#   tclsh oratcl_bench.tcl DSN USER PASS       (command-line overrides)
#
# Suggested workflow
#   tclsh oratcl_bench.tcl 2>&1 | tee before.txt
#   # rebuild oratcl with the patches
#   tclsh oratcl_bench.tcl 2>&1 | tee after.txt
#   diff before.txt after.txt
# =============================================================================

package require oratcl

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
set CFG(rows)     100000   ;# row count for §1/§2
set CFG(creps)    20000    ;# iterations for §3 cold/warm cache comparison
set CFG(bindreps) 30000    ;# iterations for §4a/4b scalar bind loop
set CFG(abatch)   500      ;# rows per -arraydml batch (§4c)
set CFG(areps)    200      ;# -arraydml batch repetitions (§4c)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc section {num title} {
    set line [string repeat "-" 72]
    puts ""
    puts $line
    puts [format "  %s. %s" $num $title]
    puts $line
    puts [format "  %-48s  %9s  %9s" "Test" "Total(ms)" "us/op"]
    puts "  [string repeat . 68]"
}

proc result_row {label n_ops elapsed_us} {
    set ms  [format "%9.1f" [expr {$elapsed_us / 1000.0}]]
    set per [expr {$n_ops > 0
                   ? [format "%9.2f" [expr {$elapsed_us / double($n_ops)}]]
                   : [format "%9s" "n/a"]}]
    puts [format "  %-48s  %s  %s" $label $ms $per]
}

# Run $script once as a warm-up (fills Tcl bytecode cache and OratCL
# internal caches), then run it again and return its wall-clock time in
# microseconds.  Runs in the CALLER'S scope via uplevel 1.
proc measure {script} {
    uplevel 1 $script
    set t0 [clock microseconds]
    uplevel 1 $script
    return [expr {[clock microseconds] - $t0}]
}

# Return a 10-column synthetic SELECT producing exactly $nrows rows.
# Mix of NUMBER and VARCHAR2 columns exercises isChar detection and
# upper_copy in the column-name cache.
proc synth_q {nrows} {
    return "
        SELECT ROWNUM                          AS id,
               'name_' || ROWNUM              AS name,
               MOD(ROWNUM, 1000)              AS val1,
               MOD(ROWNUM, 997)               AS val2,
               'text_' || TO_CHAR(ROWNUM)     AS val3,
               MOD(ROWNUM, 100)               AS val4,
               'str_'  || TO_CHAR(ROWNUM)     AS val5,
               ROWNUM * 3                     AS val6,
               'abc_'  || TO_CHAR(ROWNUM)     AS val7,
               MOD(ROWNUM, 50)                AS val8
        FROM DUAL CONNECT BY LEVEL <= $nrows"
}

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
puts "============================================================"
puts " OratCL performance benchmark"
puts "============================================================"
puts " Rows:     $CFG(rows)    (ss1, ss2)"
puts " CReps:    $CFG(creps)   (ss3 cold/warm)"
puts " BindReps: $CFG(bindreps) (ss4a/4b)"
puts " ArrBatch: $CFG(abatch) x $CFG(areps) = [expr {$CFG(abatch)*$CFG(areps)}] rows (ss4c)"
puts ""

if {[catch {set lda [oralogon $::env(ORATCL_CONNECT)]} err]} {
    puts "CONNECT FAILED: $err"; exit 1
}
puts " Connected.  (warmup run discarded; timings are the second run)"

set ROWS  $CFG(rows)
set CREPS $CFG(creps)
set Q     [synth_q $ROWS]
set Q1    [synth_q 1]
set QC    [synth_q $CREPS]

# Variable naming convention used in the timed scripts below:
#
#   _ba   — the -dataarray target (becomes an array after first orafetch)
#   _bv   — the -datavariable target (becomes a list after first orafetch)
#
# Using separate names for array vs scalar targets avoids the crash that
# results when orafetch tries to assign a list to an existing array
# variable (or vice versa) after a previous test left the same name in the
# wrong type.  _ba is always written as an array; _bv as a scalar list.

# ===========================================================================
# §1 — orafetch: single-row-per-call, multiple output modes
#
# Without -max or -returnrows, orafetch defaults to maxRows=1, so the
# while-loop calls orafetch once per row — worst case for redundant metadata
# work and best case for demonstrating the column-cache patch.
#
# Column cache (patch §2):
#   BEFORE: SnapshotColumnMeta (10x dpiStmt_getQueryInfo) + upper_copy
#           (10x DString+UtfToUpper+NewStringObj) + ObjPrintf "0".."9"
#           called on EVERY orafetch invocation.
#   AFTER:  All of the above paid ONCE per orasql; subsequent invocations
#           do only 10x IncrRefCount to borrow the cached objects.
#
# Lazy rowObj (patch §1):
#   NOT built for 1a/1b/1c/1e — eliminates N+1 allocs/appends per row.
#   Built for 1d/1f — regression checks; must not change.
# ===========================================================================
section 1 "orafetch single-row-per-call  \[column cache + lazy rowObj\]"

# 1a. Bare status-code: no variable assignment at all.
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    while {[orafetch $s] != 1403} {}
    orasql $s $Q
}]
result_row "1a. status-code, no output" $ROWS $us
oraclose $s

# 1b. -dataarray -indexbyname  [most common bulk pattern]
#     lazy rowObj NOT built; colNames borrowed from cache (patched).
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    while {[orafetch $s -dataarray _ba -indexbyname] != 1403} {}
    orasql $s $Q
}]
unset -nocomplain _ba
result_row "1b. -dataarray -indexbyname" $ROWS $us
oraclose $s

# 1c. -dataarray -indexbynumber
#     lazy rowObj NOT built; numberKeys borrowed from cache (patched).
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    while {[orafetch $s -dataarray _ba -indexbynumber] != 1403} {}
    orasql $s $Q
}]
unset -nocomplain _ba
result_row "1c. -dataarray -indexbynumber" $ROWS $us
oraclose $s

# 1d. -datavariable  [REGRESSION CHECK — rowObj IS built and assigned]
#     Uses _bv (scalar) to avoid clash with _ba (array) from 1b/1c.
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    while {[orafetch $s -datavariable _bv] != 1403} {}
    orasql $s $Q
}]
unset -nocomplain _bv
result_row "1d. -datavariable  (rowObj built -- regression check)" $ROWS $us
oraclose $s

# 1e. -command with no -datavariable
#     lazy rowObj NOT built; column cache applies; callback is a no-op.
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    orafetch $s -command {set _dummy 0}
    orasql $s $Q
}]
result_row "1e. -command only  (rowObj NOT built)" $ROWS $us
oraclose $s

# 1f. -command -datavariable  [REGRESSION CHECK — rowObj IS built]
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    orafetch $s -datavariable _bv -command {set _dummy 0}
    orasql $s $Q
}]
unset -nocomplain _bv
result_row "1f. -command -datavariable  (rowObj built -- regression)" $ROWS $us
oraclose $s

# ===========================================================================
# §2 — orafetch: bulk multi-row modes
#
# -returnrows and -resultvariable collect all rows before returning.
# rowObj is built and appended for EVERY row; lazy-rowObj patch has no
# effect here.  Column cache still eliminates metadata overhead.
# These tests verify no regression.
# ===========================================================================
section 2 "orafetch bulk multi-row  \[regression: rowObj always built\]"

# 2a. -returnrows
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    set _bv [orafetch $s -returnrows]
    orasql $s $Q
}]
unset -nocomplain _bv
result_row "2a. -returnrows  (list of row-lists)" $ROWS $us
oraclose $s

# 2b. -resultvariable
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    orafetch $s -resultvariable _bv
    orasql $s $Q
}]
unset -nocomplain _bv
result_row "2b. -resultvariable" $ROWS $us
oraclose $s

# 2c. -asdict -returnrows  (column names appear in every row dict)
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    set _bv [orafetch $s -asdict -returnrows]
    orasql $s $Q
}]
unset -nocomplain _bv
result_row "2c. -asdict -returnrows" $ROWS $us
oraclose $s

# 2d. -returnrows -max N  (single orafetch call collects all rows)
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    set _bv [orafetch $s -returnrows -max $ROWS]
    orasql $s $Q
}]
unset -nocomplain _bv
result_row "2d. -returnrows -max $ROWS  (single call)" $ROWS $us
oraclose $s

# ===========================================================================
# §3 — Column cache: forced cold vs natural warm
#
# Isolates the per-call column-metadata cost.
#
#   Cold (3a): orasql before every single-row fetch.
#              In the patched version orasql calls Oradpi_FreeFetchCache,
#              so every fetch pays the full cache-miss cost.
#              In the unpatched version there is no cache, so every fetch
#              also pays the full metadata cost.
#
#   Warm (3b): orasql only between measure-runs (to reset the cursor).
#              The warmup run fills the cache on its very first row; all
#              remaining rows in both the warmup and timed run hit the cache.
#              In the unpatched version every row still pays full metadata.
#
# Expected results:
#   BEFORE patch:  3a ≈ 3b  (no cache in either path; orasql adds overhead)
#   AFTER  patch:  3a > 3b  (3b benefits from cache; 3a misses every time)
#
# The per-row difference (3a_us - 3b_us) ≈ cost of one SnapshotColumnMeta
# + 10x upper_copy + 10x ObjPrintf before the patch.
# ===========================================================================
section 3 "Column cache cold vs warm  \[primary cache isolation test\]"

# 3a. COLD: orasql (clears cache) + single-row fetch, CREPS times.
set s [oraopen $lda]
set us [measure {
    for {set _i 0} {$_i < $CREPS} {incr _i} {
        orasql $s $Q1
        orafetch $s -dataarray _ba -indexbyname
    }
}]
unset -nocomplain _ba
result_row "3a. cold: orasql+fetch-1  x$CREPS" $CREPS $us
oraclose $s

# 3b. WARM: fetch CREPS rows from a CREPS-row query.
#     orasql at end resets cursor for the next measure-run; does NOT clear
#     the column cache in the patched version (orasql does clear it, but
#     the first row of the timed run rebuilds it immediately, and the
#     remaining CREPS-1 rows hit the warm cache).
set s [oraopen $lda]; orasql $s $QC
set us [measure {
    while {[orafetch $s -dataarray _ba -indexbyname] != 1403} {}
    orasql $s $QC
}]
unset -nocomplain _ba
result_row "3b. warm: fetch-$CREPS rows  (cache hot from row 2)" $CREPS $us
oraclose $s

# 3c. REAL LOOP on main query — same as 1b, repeated here for direct diff
#     with 3a and 3b without having to scroll back.
set s [oraopen $lda]; orasql $s $Q
set us [measure {
    while {[orafetch $s -dataarray _ba -indexbyname] != 1403} {}
    orasql $s $Q
}]
unset -nocomplain _ba
result_row "3c. real loop x$ROWS  (mirrors 1b, for direct 3a diff)" $ROWS $us
oraclose $s

# ===========================================================================
# §4 — Bind performance
#
# Double-strlen patch: BindVarByNameDual / BindValueByNameDual previously
# called strlen(nameNoColon) twice — once inside Oradpi_WithColon and again
# in the bare-name fallback.  After the patch the length is reused from
# Oradpi_WithColon's return value (m-1) for all names that fit in buf[256].
# ===========================================================================
section 4 "Bind performance  \[double-strlen fix\]"

# 4a. orabind + oraexec, 5 named binds
set s [oraopen $lda]
oraparse $s {SELECT :v1 + :v2 + :v3 + :v4 + :v5 FROM DUAL}
set us [measure {
    for {set _i 0} {$_i < $CFG(bindreps)} {incr _i} {
        orabind $s :v1 $_i \
                   :v2 [expr {$_i + 1}] \
                   :v3 [expr {$_i + 2}] \
                   :v4 [expr {$_i + 3}] \
                   :v5 [expr {$_i + 4}]
        oraexec $s
    }
}]
result_row "4a. orabind + oraexec  (5 binds x$CFG(bindreps))" $CFG(bindreps) $us
oraclose $s

# 4b. orabindexec scalar — combined bind+exec in one call
set s [oraopen $lda]
oraparse $s {SELECT :v1 + :v2 + :v3 + :v4 + :v5 FROM DUAL}
set us [measure {
    for {set _i 0} {$_i < $CFG(bindreps)} {incr _i} {
        orabindexec $s :v1 $_i \
                       :v2 [expr {$_i + 1}] \
                       :v3 [expr {$_i + 2}] \
                       :v4 [expr {$_i + 3}] \
                       :v5 [expr {$_i + 4}]
    }
}]
result_row "4b. orabindexec scalar  (5 binds x$CFG(bindreps))" $CFG(bindreps) $us
oraclose $s

# 4c. orabindexec -arraydml — batch DML path.
#     The arraydml path now uses Tcl_ListObjIndex with IncrRefCount/DecrRefCount
#     per element instead of a pre-materialised elems[] pointer array.
set arrSetup [oraopen $lda]
catch {orasql $arrSetup {DROP TABLE oratcl_bench_tmp}}
if {[catch {
    orasql $arrSetup {
        CREATE GLOBAL TEMPORARY TABLE oratcl_bench_tmp (
            id   NUMBER,
            name VARCHAR2(64),
            val  NUMBER
        ) ON COMMIT DELETE ROWS
    }
    oraclose $arrSetup

    set _ids {}; set _names {}; set _vals {}
    for {set _i 0} {$_i < $CFG(abatch)} {incr _i} {
        lappend _ids   $_i
        lappend _names "row_$_i"
        lappend _vals  [expr {$_i * 3}]
    }

    set s [oraopen $lda]
    oraparse $s {INSERT INTO oratcl_bench_tmp VALUES (:id, :name, :val)}
    set us [measure {
        for {set _r 0} {$_r < $CFG(areps)} {incr _r} {
            orabindexec $s -arraydml :id $_ids :name $_names :val $_vals
        }
    }]
    set total [expr {$CFG(abatch) * $CFG(areps)}]
    result_row "4c. orabindexec -arraydml  ($CFG(abatch)x$CFG(areps))" $total $us
    oraclose $s
} err]} {
    catch {oraclose $arrSetup}
    puts [format "  %-48s  %9s  (%s)" "4c. orabindexec -arraydml" "SKIPPED" $err]
}

# ===========================================================================
# §5 — Handle generation throughput
#
# oraopen + oraclose exercises Oradpi_NewHandleName.
# BEFORE patch: Tcl_MutexLock + increment + Tcl_MutexUnlock per handle.
# AFTER  patch: atomic_fetch_add_explicit, no lock, no memory barriers.
# ===========================================================================
section 5 "Handle generation  \[atomic counter patch\]"

set HREPS 5000
set us [measure {
    for {set _i 0} {$_i < $HREPS} {incr _i} {
        set _h [oraopen $lda]
        oraclose $_h
    }
}]
result_row "5. oraopen + oraclose  x$HREPS" $HREPS $us

# ===========================================================================
# §6 — Statement lifecycle baseline
#
# oraopen + oraparse + oraclose with no fetch or bind.
# Establishes fixed per-statement overhead; should be identical before and
# after all patches.  A change here indicates an unintended side-effect.
# ===========================================================================
section 6 "Statement lifecycle baseline  \[expect no change\]"

set LREPS 5000
set us [measure {
    for {set _i 0} {$_i < $LREPS} {incr _i} {
        set _s [oraopen $lda]
        oraparse $_s {SELECT 1 FROM DUAL}
        oraclose $_s
    }
}]
result_row "6. oraopen + oraparse + oraclose  x$LREPS" $LREPS $us

# ---------------------------------------------------------------------------
# Cleanup and disconnect
# ---------------------------------------------------------------------------
catch {
    set c [oraopen $lda]
    catch {orasql $c {DROP TABLE oratcl_bench_tmp}}
    oraclose $c
}
oralogoff $lda

puts ""
puts [string repeat "=" 72]
puts " Benchmark complete."
puts [string repeat "=" 72]
