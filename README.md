# Oratcl 9.1 — ODPI‑C for Tcl 9

A Tcl 9 extension that re‑implements the classic Oratcl API on top of **ODPI‑C 5.6.4** (no OCI dependency).

## Features

- **Thread‑pool async**: `oraexecasync` / `orawaitasync` / `orabreak` with persistent worker threads, reference‑counted entries, and orphan‑safe teardown.
- **Cross‑interpreter connection adoption**: share a physical Oracle session across Tcl interpreters with refcounted shared records, per‑connection operation gates, and behavioral policy sync.
- **Session pooling**: `oralogon -pool {min max incr}` with homogeneous/heterogeneous mode, configurable get‑mode, and tuning knobs (`-waittimeout`, `-timeout`, `-maxlifetime`, `-pinginterval`, `-pingtimeout`, `-stmtcachesize`). Multiple `oralogon -pool` calls with identical parameters share one underlying session pool process‑wide.
- **LOB helpers**: `oralob size|read|write|trim|close`, with `inlineLobs` mode for automatic materialization during fetch.
- **Driver‑side failover**: configurable retry/backoff policy (`foMaxAttempts`, `foBackoffMs`, `foBackoffFactor`, `foErrorClasses`) with debounced failover callbacks.
- **Rich diagnostics**: `oramsg` exposes `fn`, `action`, `sqlstate`, `recoverable`, `warning`, `offset` via `all`/`allx`.
- **Namespace support**: all commands available under `::oratcl::*` for `namespace import`.
- **Integer safety**: all narrowing conversions use checked helpers with descriptive errors.

## Build

```sh
./configure --with-tcl=/path/to/tcl9
make
make install
```

> **Windows (MSVC):** Use the TEA `tclconfig` files with `nmake` or generate a Visual Studio project; ensure the ODPI sources in `./odpi/src` are compiled into the same binary (or link with a static ODPI build). Define `_WIN64` on 64‑bit builds.

## Static Linking

Oratcl can be statically linked into `tclsh`. Each new interpreter calls `oratcl_Init`; the ODPI context is created once per process so Oracle client libraries are loaded only once.

## Tests

```sh
export ORATCL_CONNECT='user/pw@//host:1521/service'
tclsh9 tests/all.tcl
```

The test suite includes unit tests for all commands, multi‑interpreter adoption tests, multi‑thread safety tests, fetch reentrancy regression tests, async scoping tests, pool lifetime tests, adoption policy tests, and a stress suite.

## Documentation

- **Man page**: `doc/oratcl.n`
- **HTML reference**: `doc/oratcl.html` and `index.html` (project root)
- **What's New**: `WHATS_NEW_9_1.md`

## Quick Example

```tcl
package require oratcl 9.1

set L [oralogon "scott/tiger@//dbhost/pdb"]
set S [oraopen $L]
oraparse $S {select empno, ename from emp where deptno = :d}
orabind  $S :d 10
oraexec  $S
while 1 {
    set rc [orafetch $S -asdict -datavariable row]
    if {$rc == 1403} break
    puts "[dict get $row EMPNO] [dict get $row ENAME]"
}
oraclose $S
oralogoff $L
```

## License

See `license.terms` for usage, redistribution, and disclaimer of all warranties.
