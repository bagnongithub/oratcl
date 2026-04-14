#!/usr/bin/env tclsh
# =============================================================================
# oratcl benchmark.tcl  —  OratCL performance benchmark
#
# Covers all findings from the code-review / implementation session:
#
#   §1  orafetch single-row-per-call  (column cache, lazy rowObj)
#   §2  orafetch bulk multi-row       (regression guard)
#   §3  Column cache cold vs warm     (primary cache isolation)
#   §4  Bind performance              (getInfo cache, arraydml zero-copy)
#   §5  Handle generation             (atomic counter)
#   §6  Statement lifecycle baseline  (should be flat across builds)
#   §7  F5 — gate elimination         (scalar vs LOB queries)
#   §8  F7 — getInfo cache            (DML and SELECT exec paths)
#   §9  F3 — oraparse -novalidate       (round-trip opt-in cost)
#   §10 F6 — arraydml string zero-copy (short/medium/long strings)
#   §11 F1 — shared pool registry     (pool reuse across oralogon)
#
# Run BEFORE and AFTER the patches, save both outputs, compare.
#
# Usage
#   tclsh benchmark.tcl                    (reads ORATCL_CONNECT env)
#   tclsh benchmark.tcl user/pass@db
#   tclsh benchmark.tcl --baseline         (§1-§6 only)
#   tclsh benchmark.tcl --section 7        (one section only)
#
# Environment
#   ORATCL_CONNECT   connect string (default: scott/tiger@localhost/XE)
#   ORATCL_POOL      set to "1" to enable §11 pool tests
#   BENCH_ROWS       override CFG(rows)
# =============================================================================

package require oratcl

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
set CFG(rows)        100000
set CFG(creps)        20000
set CFG(bindreps)     30000
set CFG(abatch)         500
set CFG(areps)          200
set CFG(hreps)         5000
set CFG(lreps)         5000
set CFG(gate_rows)    50000
set CFG(dml_reps)     10000
set CFG(parse_reps)    5000
set CFG(str_batch)      200
set CFG(str_reps)       300
set CFG(pool_reps)     1000

if {[info exists ::env(BENCH_ROWS)]} { set CFG(rows) $::env(BENCH_ROWS) }
set CFG(use_pool) [expr {[info exists ::env(ORATCL_POOL)] \
                         && $::env(ORATCL_POOL) eq "1"}]

set ONLY_SECTION ""
set BASELINE_ONLY 0
foreach arg $argv {
    if {$arg eq "--baseline"} { set BASELINE_ONLY 1 }
    if {[string match --section=* $arg]} {
        set ONLY_SECTION [string range $arg 10 end]
    }
}
if {[llength $argv] == 1 && [string index [lindex $argv 0] 0] ne "-"} {
    set ::env(ORATCL_CONNECT) [lindex $argv 0]
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
proc section {num title} {
    global ONLY_SECTION
    if {$ONLY_SECTION ne "" && $ONLY_SECTION ne $num} { return 0 }
    set line [string repeat "-" 72]
    puts ""
    puts $line
    puts [format "  %s. %s" $num $title]
    puts $line
    puts [format "  %-50s  %9s  %9s" "Test" "Total(ms)" "us/op"]
    puts "  [string repeat . 70]"
    return 1
}

proc result_row {label n_ops elapsed_us} {
    set ms  [format "%9.1f" [expr {$elapsed_us / 1000.0}]]
    set per [expr {$n_ops > 0
                   ? [format "%9.2f" [expr {$elapsed_us / double($n_ops)}]]
                   : [format "%9s" "n/a"]}]
    puts [format "  %-50s  %s  %s" $label $ms $per]
}

proc result_note {msg} {
    puts [format "  %s" $msg]
}

proc measure {script} {
    uplevel 1 $script
    set t0 [clock microseconds]
    uplevel 1 $script
    return [expr {[clock microseconds] - $t0}]
}

proc synth_q {nrows} {
    return "
        SELECT ROWNUM                       AS id,
               'name_' || ROWNUM           AS name,
               MOD(ROWNUM, 1000)           AS val1,
               MOD(ROWNUM, 997)            AS val2,
               'text_' || TO_CHAR(ROWNUM) AS val3,
               MOD(ROWNUM, 100)            AS val4,
               'str_'  || TO_CHAR(ROWNUM) AS val5,
               ROWNUM * 3                 AS val6,
               'abc_'  || TO_CHAR(ROWNUM) AS val7,
               MOD(ROWNUM, 50)            AS val8
        FROM DUAL CONNECT BY LEVEL <= $nrows"
}

proc synth_scalar_q {nrows} {
    return "
        SELECT ROWNUM AS n1, ROWNUM*2 AS n2, ROWNUM*3 AS n3,
               ROWNUM*4 AS n4, ROWNUM*5 AS n5
        FROM DUAL CONNECT BY LEVEL <= $nrows"
}

proc ensure_bench_table {lda} {
    set c [oraopen $lda]
    catch {orasql $c {DROP TABLE oratcl_bench_tmp}}
    set rc [catch {
        orasql $c {
            CREATE GLOBAL TEMPORARY TABLE oratcl_bench_tmp (
                id   NUMBER,
                name VARCHAR2(256),
                val  NUMBER
            ) ON COMMIT DELETE ROWS
        }
    } err]
    oraclose $c
    if {$rc} { error "Cannot create bench table: $err" }
}

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
puts "============================================================"
puts " OratCL performance benchmark"
puts "============================================================"
puts " Rows:      $CFG(rows)    (§1/§2)"
puts " CReps:     $CFG(creps)   (§3)"
puts " BindReps:  $CFG(bindreps) (§4a/4b)"
puts " ArrBatch:  $CFG(abatch)x$CFG(areps) = [expr {$CFG(abatch)*$CFG(areps)}] (§4c)"
puts " GateRows:  $CFG(gate_rows) (§7)"
puts " DMLReps:   $CFG(dml_reps) (§8)"
puts ""

set connstr [expr {[info exists ::env(ORATCL_CONNECT)]
                   ? $::env(ORATCL_CONNECT) : "scott/tiger@localhost/XE"}]
if {[catch {set lda [oralogon $connstr]} err]} {
    puts "CONNECT FAILED: $err"; exit 1
}
puts " Connected.  (warmup run discarded; timings are the second run)"

set ROWS  $CFG(rows)
set CREPS $CFG(creps)
set Q     [synth_q $ROWS]
set Q1    [synth_q 1]
set QC    [synth_q $CREPS]

# ===========================================================================
# §1 — orafetch single-row-per-call
# ===========================================================================
if {[section 1 "orafetch single-row-per-call  \[column cache + lazy rowObj\]"]} {

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        while {[orafetch $s] != 1403} {}
        orasql $s $Q
    }]
    result_row "1a. status-code, no output" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        while {[orafetch $s -dataarray _ba -indexbyname] != 1403} {}
        orasql $s $Q
    }]
    unset -nocomplain _ba
    result_row "1b. -dataarray -indexbyname" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        while {[orafetch $s -dataarray _ba -indexbynumber] != 1403} {}
        orasql $s $Q
    }]
    unset -nocomplain _ba
    result_row "1c. -dataarray -indexbynumber" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        while {[orafetch $s -datavariable _bv] != 1403} {}
        orasql $s $Q
    }]
    unset -nocomplain _bv
    result_row "1d. -datavariable  (rowObj built -- regression)" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        orafetch $s -command {set _dummy 0}
        orasql $s $Q
    }]
    result_row "1e. -command only  (Oracle I/O floor)" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        orafetch $s -datavariable _bv -command {set _dummy 0}
        orasql $s $Q
    }]
    unset -nocomplain _bv
    result_row "1f. -command -datavariable  (rowObj built -- regression)" $ROWS $us
    oraclose $s
}

# ===========================================================================
# §2 — orafetch bulk multi-row
# ===========================================================================
if {[section 2 "orafetch bulk multi-row  \[regression guard\]"]} {

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        set _bv [orafetch $s -returnrows]
        orasql $s $Q
    }]
    unset -nocomplain _bv
    result_row "2a. -returnrows  (list of row-lists)" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        orafetch $s -resultvariable _bv
        orasql $s $Q
    }]
    unset -nocomplain _bv
    result_row "2b. -resultvariable" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        set _bv [orafetch $s -asdict -returnrows]
        orasql $s $Q
    }]
    unset -nocomplain _bv
    result_row "2c. -asdict -returnrows" $ROWS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        set _bv [orafetch $s -returnrows -max $ROWS]
        orasql $s $Q
    }]
    unset -nocomplain _bv
    result_row "2d. -returnrows -max $ROWS  (single call)" $ROWS $us
    oraclose $s

    # 2e exercises the batchLimit fix from the test_latest.log bug
    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        set _all {}
        while 1 {
            set rc [orafetch $s -resultvariable _page -max 1000]
            foreach r $_page { lappend _all $r }
            if {$rc == 1403} break
        }
        orasql $s $Q
    }]
    unset -nocomplain _all _page
    result_row "2e. -resultvariable -max 1000 loop  (batchLimit fix)" $ROWS $us
    oraclose $s
}

# ===========================================================================
# §3 — Column cache cold vs warm
# ===========================================================================
if {[section 3 "Column cache cold vs warm  \[primary cache isolation\]"]} {

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

    set s [oraopen $lda]; orasql $s $QC
    set us [measure {
        while {[orafetch $s -dataarray _ba -indexbyname] != 1403} {}
        orasql $s $QC
    }]
    unset -nocomplain _ba
    result_row "3b. warm: fetch-$CREPS rows  (cache hot from row 2)" $CREPS $us
    oraclose $s

    set s [oraopen $lda]; orasql $s $Q
    set us [measure {
        while {[orafetch $s -dataarray _ba -indexbyname] != 1403} {}
        orasql $s $Q
    }]
    unset -nocomplain _ba
    result_row "3c. real loop x$ROWS  (mirrors 1b)" $ROWS $us
    oraclose $s
}

# ===========================================================================
# §4 — Bind performance
# ===========================================================================
if {[section 4 "Bind performance"]} {

    set s [oraopen $lda]
    oraparse $s {SELECT :v1 + :v2 + :v3 + :v4 + :v5 FROM DUAL}
    set us [measure {
        for {set _i 0} {$_i < $CFG(bindreps)} {incr _i} {
            orabind $s :v1 $_i :v2 [expr {$_i+1}] :v3 [expr {$_i+2}] \
                       :v4 [expr {$_i+3}] :v5 [expr {$_i+4}]
            oraexec $s
        }
    }]
    result_row "4a. orabind + oraexec  (5 binds x$CFG(bindreps))" $CFG(bindreps) $us
    oraclose $s

    set s [oraopen $lda]
    oraparse $s {SELECT :v1 + :v2 + :v3 + :v4 + :v5 FROM DUAL}
    set us [measure {
        for {set _i 0} {$_i < $CFG(bindreps)} {incr _i} {
            orabindexec $s :v1 $_i :v2 [expr {$_i+1}] :v3 [expr {$_i+2}] \
                           :v4 [expr {$_i+3}] :v5 [expr {$_i+4}]
        }
    }]
    result_row "4b. orabindexec scalar  (5 binds x$CFG(bindreps))" $CFG(bindreps) $us
    oraclose $s

    if {[catch {ensure_bench_table $lda} etbl]} {
        result_note "4c. SKIPPED: $etbl"
    } else {
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
    }
}

# ===========================================================================
# §5 — Handle generation
# ===========================================================================
if {[section 5 "Handle generation  \[atomic counter\]"]} {
    set us [measure {
        for {set _i 0} {$_i < $CFG(hreps)} {incr _i} {
            set _h [oraopen $lda]; oraclose $_h
        }
    }]
    result_row "5. oraopen + oraclose  x$CFG(hreps)" $CFG(hreps) $us
}

# ===========================================================================
# §6 — Statement lifecycle baseline
# ===========================================================================
if {[section 6 "Statement lifecycle baseline  \[flat across builds\]"]} {

    # 6a. Plain oraparse — client-side only, no round-trip (F3 default)
    set us [measure {
        for {set _i 0} {$_i < $CFG(lreps)} {incr _i} {
            set _s [oraopen $lda]
            oraparse $_s {SELECT 1 FROM DUAL}
            oraclose $_s
        }
    }]
    result_row "6a. oraopen+oraparse+oraclose  x$CFG(lreps)" $CFG(lreps) $us

    # 6b. oraparse -novalidate — skips PARSE_ONLY round-trip (F3 fast path)
    set us [measure {
        for {set _i 0} {$_i < $CFG(lreps)} {incr _i} {
            set _s [oraopen $lda]
            oraparse $_s -novalidate {SELECT 1 FROM DUAL}
            oraclose $_s
        }
    }]
    result_row "6b. oraparse -novalidate (no round-trip)  x$CFG(lreps)" $CFG(lreps) $us
}

if {$BASELINE_ONLY} {
    catch { set c [oraopen $lda]; orasql $c {DROP TABLE oratcl_bench_tmp}; oraclose $c }
    oralogoff $lda
    puts ""
    puts [string repeat "=" 72]
    puts " Baseline complete."
    puts [string repeat "=" 72]
    exit 0
}

# ===========================================================================
# §7 — F5: Gate elimination for scalar vs LOB queries
#
# After F5, orafetch fast-path acquires the connection gate ONLY when
# fetchHasLobCols==1.  For scalar-only queries the gate acquire/release
# per row is eliminated.
#
# Interpretation:
#   BEFORE F5:  7a ≈ 7b  (gate always acquired regardless of column type)
#   AFTER  F5:  7a < 7b  (7a skips gate; 7b still needs it for dpiLob_addRef)
#               7a ≈ §1e (Oracle I/O floor, almost all of it is network)
#
# If 7a and 7b are identical after the patch, suspect the LOB query is
# falling back to the getQueryValue path (fetchVarData==NULL), which means
# the fast-path var-build failed for the CLOB type — check for object columns.
# ===========================================================================
if {[section 7 "F7/F5 — Gate elimination: scalar vs LOB columns"]} {

    set GR $CFG(gate_rows)
    set Qs [synth_scalar_q $GR]

    # 7a. All-scalar (5 NUMBER cols) — fetchHasLobCols=0, gate skipped
    set s [oraopen $lda]; orasql $s $Qs
    set us [measure {
        while {[orafetch $s -dataarray _ba -indexbynumber] != 1403} {}
        orasql $s $Qs
    }]
    unset -nocomplain _ba
    result_row "7a. scalar-only 5×NUMBER  (NO gate per row after F5)" $GR $us
    oraclose $s

    # 7b. CLOB column — fetchHasLobCols=1, gate still acquired per row
    set lob_q "SELECT ROWNUM AS n, TO_CLOB('x' || TO_CHAR(ROWNUM)) AS c
               FROM DUAL CONNECT BY LEVEL <= $GR"
    if {[catch {
        set s [oraopen $lda]; orasql $s $lob_q
        set us [measure {
            while {[orafetch $s -dataarray _ba -indexbynumber] != 1403} {}
            orasql $s $lob_q
        }]
        unset -nocomplain _ba
        result_row "7b. CLOB col  (gate acquired per row)" $GR $us
        oraclose $s
    } err]} {
        result_note "7b. CLOB unavailable — $err"
    }

    # 7c. NUMBER+CLOB mix — confirms fetchHasLobCols triggers gated path
    set mix_q "SELECT ROWNUM AS n, MOD(ROWNUM,100) AS m,
               TO_CLOB('row') AS c
               FROM DUAL CONNECT BY LEVEL <= $GR"
    if {[catch {
        set s [oraopen $lda]; orasql $s $mix_q
        set us [measure {
            while {[orafetch $s -dataarray _ba -indexbynumber] != 1403} {}
            orasql $s $mix_q
        }]
        unset -nocomplain _ba
        result_row "7c. NUMBER+CLOB mix  (gate acquired per row)" $GR $us
        oraclose $s
    } err]} {
        result_note "7c. mixed LOB unavailable — $err"
    }

    result_note ""
    result_note "Expected after F5: 7a < 7b ≈ 7c  (gate eliminated for 7a)"
    result_note "Before F5:         7a ≈ 7b ≈ 7c  (gate always held)"
}

# ===========================================================================
# §8 — F7: getInfo cache — DML and SELECT exec paths
#
# Before F7: every oraexec / orabindexec acquires the gate, calls
# dpiStmt_getInfo (isDML/isPLSQL for autocommit), releases the gate —
# even for SELECT statements that never commit.
#
# After F7: UpdateStmtType populates stmtIsDML/stmtIsPLSQL once after
# parse/execute; hot-path reads the cached fields — zero ODPI calls.
#
# 8a: INSERT — DML path; getInfo was most meaningful here (BATCH_ERRORS flag)
# 8b: SELECT — query path; getInfo result always false, eliminating it is pure win
# 8c: First vs warm — measures the one UpdateStmtType call amortised over N execs
# ===========================================================================
if {[section 8 "F7 — getInfo cache: DML and SELECT exec paths"]} {

    if {[catch {ensure_bench_table $lda} etbl]} {
        result_note "§8 SKIPPED: $etbl"
    } else {
        # 8a. INSERT repeated, autocommit OFF
        set s [oraopen $lda]
        oraparse $s {INSERT INTO oratcl_bench_tmp (id,name,val) VALUES (:id,:name,:val)}
        set us [measure {
            for {set _i 0} {$_i < $CFG(dml_reps)} {incr _i} {
                orabind $s :id $_i :name "r$_i" :val $_i
                oraexec $s
            }
        }]
        result_row "8a. INSERT oraexec x$CFG(dml_reps)  (DML, no autocommit)" \
                   $CFG(dml_reps) $us
        oraclose $s

        # 8b. SELECT repeated — stmtIsDML=0 cached, never pays for check
        set s [oraopen $lda]
        oraparse $s {SELECT 1+:v FROM DUAL}
        set us [measure {
            for {set _i 0} {$_i < $CFG(dml_reps)} {incr _i} {
                orabind $s :v $_i
                oraexec $s
            }
        }]
        result_row "8b. SELECT oraexec x$CFG(dml_reps)  (query, getInfo-free)" \
                   $CFG(dml_reps) $us
        oraclose $s

        # 8c. First-exec (cold cache) vs warm-cache exec
        set FN 1000
        set us_first [measure {
            for {set _i 0} {$_i < $FN} {incr _i} {
                set _s [oraopen $lda]
                oraparse $_s {INSERT INTO oratcl_bench_tmp (id,name,val) VALUES(:a,:b,:c)}
                orabind $_s :a $_i :b "x" :c 0
                oraexec $_s
                oraclose $_s
            }
        }]
        set _s [oraopen $lda]
        oraparse $_s {INSERT INTO oratcl_bench_tmp (id,name,val) VALUES(:a,:b,:c)}
        set us_warm [measure {
            for {set _i 0} {$_i < $FN} {incr _i} {
                orabind $_s :a $_i :b "x" :c 0
                oraexec $_s
            }
        }]
        oraclose $_s
        result_row "8c-first. open+parse+exec (cold cache)  x$FN" $FN $us_first
        result_row "8c-warm.  rebind+exec (warm cache)       x$FN" $FN $us_warm
        set delta [format "%.1f" \
            [expr {($us_first - $us_warm) / double($FN)}]]
        result_note "  8c-delta: ${delta}us/call (≈ one UpdateStmtType cost amortised)"
    }
}

# ===========================================================================
# §9 — F3: oraparse -novalidate round-trip cost
#
# Plain oraparse (F3 default): dpiConn_prepareStmt only — no round-trip.
# oraparse -novalidate:          dpiConn_prepareStmt + DPI_MODE_EXEC_PARSE_ONLY.
#
# The round-trip marginal cost tells you at what execution-count it becomes
# cheaper to skip -validate at startup vs the time saved by catching a syntax
# error before the first execute.
#
# For OLTP (parse once, execute 10k times): skip -validate; first-execute
# error is fast to detect, and the parse round-trip is wasted 100% of the time.
#
# For batch jobs (parse once, execute 1-2 times): -validate is worth it if the
# SQL is user-provided or generated, since finding the error before the INSERT
# loop starts saves debug time.
# ===========================================================================
if {[section 9 "F3 — oraparse -novalidate: round-trip cost"]} {

    set N $CFG(parse_reps)

    # 9a. Default oraparse: includes PARSE_ONLY round-trip (backward compatible)
    set us_validate [measure {
        for {set _i 0} {$_i < $N} {incr _i} {
            set _s [oraopen $lda]
            oraparse $_s {SELECT :v1 + :v2 FROM DUAL}
            oraclose $_s
        }
    }]
    result_row "9a. oraparse (default, round-trip)  x$N" $N $us_validate

    # 9b. oraparse -novalidate: skips round-trip; errors surface at execute time
    set us_novalidate [measure {
        for {set _i 0} {$_i < $N} {incr _i} {
            set _s [oraopen $lda]
            oraparse $_s -novalidate {SELECT :v1 + :v2 FROM DUAL}
            oraclose $_s
        }
    }]
    result_row "9b. oraparse -novalidate (skip round-trip)  x$N" $N $us_novalidate

    set diff_us [expr {$us_validate - $us_novalidate}]
    set per_us  [format "%.1f" [expr {$diff_us / double($N)}]]
    set diff_ms [format "%.1f" [expr {$diff_us / 1000.0}]]
    result_note "  9c. round-trip saving with -novalidate: ${per_us}us/call  (${diff_ms}ms over $N calls)"
    result_note "  Break-even: -validate is free after ~[format %.0f \
        [expr {$diff_us / max(1.0, ([synth_q 1] ne "" ? 0.1 : 0.1))}]] executes"
}

# ===========================================================================
# §10 — F6: arraydml BYTES zero-copy
#
# Before F6: each VARCHAR2 element copies to a fresh heap buffer per row.
# After F6:  Tcl_Obj* is pinned; dpiData.asBytes.ptr points at Tcl buffer.
#
# Benefit scales with string length × batch_size × repeat_count.
# ===========================================================================
if {[section 10 "F6 — arraydml BYTES zero-copy"]} {

    if {[catch {ensure_bench_table $lda} etbl]} {
        result_note "§10 SKIPPED: $etbl"
    } else {
        set BS $CFG(str_batch)
        set BR $CFG(str_reps)
        set total [expr {$BS * $BR}]

        foreach {label avg_len} {
            "10a. short  strings (8-byte avg)" 8
            "10b. medium strings (64-byte avg)" 64
            "10c. long   strings (200-byte avg)" 200
        } {
            set pad [string repeat "x" $avg_len]
            set _ids {}; set _names {}; set _vals {}
            for {set _i 0} {$_i < $BS} {incr _i} {
                lappend _ids   $_i
                lappend _names [string range "${_i}${pad}" 0 [expr {$avg_len-1}]]
                lappend _vals  [expr {$_i % 1000}]
            }
            set s [oraopen $lda]
            oraparse $s {INSERT INTO oratcl_bench_tmp VALUES (:id, :name, :val)}
            set us [measure {
                for {set _r 0} {$_r < $BR} {incr _r} {
                    orabindexec $s -arraydml :id $_ids :name $_names :val $_vals
                }
            }]
            result_row $label $total $us
            oraclose $s
        }
        result_note "  Expected: 10c > 10b > 10a before F6 (copy cost scales with length)"
        result_note "  After F6: differences compress; only ODPI execute cost remains"
    }
}

# ===========================================================================
# §11 — F1: Shared pool registry + F8: pool tuning surface
#
# Before F1: each oralogon -pool calls dpiPool_create (round-trip, OCI auth).
# After F1:  repeated calls with the same parameters reuse one dpiPool*,
#            skipping dpiPool_create entirely.
#
# 11a: First oralogon -pool — always creates the pool; establishes baseline.
# 11b: N repeated calls with same params — should be << 11a after F1.
# 11c: Different params — must create a new pool (cache miss).
# 11e: F8 tuning knobs accepted without error.
#
# Only runs when ORATCL_POOL=1 (set env var to enable).
# ===========================================================================
if {$CFG(use_pool) && [section 11 "F1 — Shared pool registry  +  F8 pool tuning"]} {

    # 11a. Baseline: first pool create (always pays dpiPool_create cost)
    set us_first [measure {
        set _lda1 [oralogon $connstr -pool {1 4 1}]
        oralogoff $_lda1
    }]
    result_row "11a. first oralogon -pool  (pool created)" 1 $us_first

    # 11b. Reuse: same params → registry hit → skip dpiPool_create
    set us_reuse [measure {
        for {set _i 0} {$_i < $CFG(pool_reps)} {incr _i} {
            set _lda2 [oralogon $connstr -pool {1 4 1}]
            oralogoff $_lda2
        }
    }]
    result_row "11b. oralogon -pool reuse  x$CFG(pool_reps)" $CFG(pool_reps) $us_reuse

    # 11c. Cache miss: different pool size → new dpiPool_create
    set us_new [measure {
        set _lda3 [oralogon $connstr -pool {1 8 2}]
        oralogoff $_lda3
    }]
    result_row "11c. oralogon -pool new params  (new pool)" 1 $us_new

    set per_reuse [expr {$us_reuse / double($CFG(pool_reps))}]
    set saving_us [format "%.1f" [expr {$us_first - $per_reuse}]]
    result_note "  11d. pool-create saved per reuse: ${saving_us}us"
    result_note "       (11b/rep << 11a means F1 is working; 11c ≈ 11a)"

    # 11e. F8: verify pool tuning options are accepted and don't crash
    if {[catch {
        set _lda4 [oralogon $connstr -pool {1 4 1} \
                   -waittimeout  2000 \
                   -timeout      300  \
                   -maxlifetime  3600 \
                   -pinginterval  60  \
                   -pingtimeout   500 \
                   -stmtcachesize  20]
        oralogoff $_lda4
        result_note "  11e. F8 pool tuning knobs accepted: OK"
    } err]} {
        result_note "  11e. F8 pool tuning FAILED: $err"
    }

    # 11f. timedwait + waitTimeout (the bug fixed by F8: default was 0ms)
    if {[catch {
        set _lda5 [oralogon $connstr -pool {1 4 1} \
                   -getmode timedwait -waittimeout 3000]
        oralogoff $_lda5
        result_note "  11f. timedwait + -waittimeout 3000ms accepted: OK"
    } err]} {
        result_note "  11f. timedwait with waitTimeout FAILED: $err"
    }

} elseif {!$CFG(use_pool)} {
    if {[section 11 "F1/F8 — Pool registry/tuning  (set ORATCL_POOL=1 to run)"]} {
        result_note "Skipped.  Export ORATCL_POOL=1 to run pool tests."
    }
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
catch { set c [oraopen $lda]; orasql $c {DROP TABLE oratcl_bench_tmp}; oraclose $c }
oralogoff $lda

puts ""
puts [string repeat "=" 72]
puts " Benchmark complete."
puts [string repeat "=" 72]
