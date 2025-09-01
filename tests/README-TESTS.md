# Oratcl 9.0 Test Suite

This folder contains a tcltest 2.5–compliant test suite for the Oratcl 9.0 (ODPI-C) library.

## Prerequisites

- Tcl 9.0 with tcltest 2.5
- A reachable Oracle database. Provide the connect string in the `ORATCL_CONNECT` environment variable, e.g.
  `export ORATCL_CONNECT='scott/tiger@dbhost/pdb1'`.
- The Oratcl package must be locatable by `package require oratcl 9.0`. If you are running directly from the
  source tree, `00-setup.test` tries to add the repo root and `lib/` to `auto_path` automatically.

## Running

From the project root (one level above this folder):

```sh
tclsh tests/all.tcl     # runs everything except optional constraints
```

The test runner uses numeric prefixes (`00-…`, `01-…`, …) to enforce order where needed.

## Categories Covered

- **Conformance**: every public command is exercised (`oralogon`, `oraconfig`, `orainfo`, `oraopen`/`orastmt`,
  `oraclose`, `oraparse`, `orasql`, `orabind`, `orabindexec`, `oraexec`, `oraexecasync`, `orawaitasync`,
  `orafetch`, `oracols`, `oradesc`, `oramsg`, `oralob`, `oraautocommit`, `oracommit`, `orarollback`, `orabreak`).
- **Multi-interp**: `20-multi-interp.test` creates child interpreters and verifies handle adoption is safe.
- **Multi-thread**: `30-multi-thread.test` uses the `Thread` package to run queries in parallel.
- **Multi-interp + Multi-thread**: `40-multi-both.test` combines both.
- **Memory**: `90-memory.test` provides coarse checks under the `memory` constraint on posix systems.

## Notes

- The async tests aim to be robust across environments. If a "heavy" query completes too quickly, the
  test still passes as long as the async API behaves consistently.
- LOB tests verify both **handle** mode (`inlineLobs 0`) and **inline** mode (`inlineLobs 1`).
- Tables created by tests are prefixed with `ORATCL9_` and include PID and a sequence to avoid collisions.


Note: For `30-multi-thread.test` and `40-multi-both.test`, the Thread package must be available.
