# Oratcl ODPI‑C for Tcl 9

This is a Tcl 9 extension that re‑implements Oratcl on top of **ODPI‑C 5.6.2** (no OCI).

- Async (`oraexecasync`, `orawaitasync`, `orabreak`)
- LOB helpers (`oralob size|read|write|trim|close`), with `inlineLobs` option
- Extended `oramsg` fields (`fn`, `action`, `sqlstate`, `recoverable`, `warning`, `offset` via `allx`)

## Build

```sh
./configure --with-tcl=/path/to/tcl9
make
```

> **Windows (MSVC):** Use the TEA `tclconfig` files with `nmake` or generate a Visual Studio project; ensure the ODPI sources in `./odpi/src` are compiled into the same binary (or link with a static ODPI build). Define `_WIN64` on 64‑bit builds.

## Static linking

You can statically link Oratcl into `tclsh`. Each new interpreter will call `oratcl_Init`; Oratcl creates the ODPI context once per process, so Oracle client libraries are only loaded once.

## Tests

```sh
export ORATCL_CONNECT='user/pw@//host:1521/service'
tclsh9 tests/all.tcl
```

## Documentation

See `doc/oratcl.n` for the full command reference.
