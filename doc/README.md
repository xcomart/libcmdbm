# API reference

The reference is generated from the doc comments in
[`src/libcmdbm.h`](../src/libcmdbm.h), which is the whole public API — the
`.c` files, the DBMS modules and the internal headers are implementation and
are deliberately left out.

## Building it

```bash
cd doc && doxygen
```

or through the build, which also fills in the version number:

```bash
cmake -S . -B build
cmake --build build --target cmdbm_docs
```

The target appears only when doxygen is installed, and it is never part of
`all` — the reference is built on request. It is called `cmdbm_docs` rather
than `docs` because the libcmutils submodule defines a `docs` target of its
own, and two targets of one name do not configure. Output lands in `doc/html`;
open `doc/html/index.html`. Neither the output nor `doxygen.log` is committed.

Graphviz is not needed. Doxygen 1.9 or newer is expected; older versions still
work but ignore some of the HTML settings.

## What it contains

The front page covers what has to be known before reading anything else: that
an object is a struct of function pointers reached through `CMCall`, which
three objects an application deals with, and the
`CMDBM_Init` / `CMDBM_Clear` lifecycle.

**Topics** is the way in. The API is grouped by subject rather than listed
alphabetically, and within a group the declaration order of the header is kept,
so a type is followed by its methods and then by its constructor:

| Topic | Covers |
| --- | --- |
| Library initialization and version | `CMDBM_Init`, `CMDBM_Clear`, `CMDBM_GetLibVersion` |
| DBMS modules and the module interface | `CMDBM_ModuleInterface` and its 20 callbacks, `CMDBM_RegisterDBMS` |
| Datasources and their connection pool | `CMDBM_PoolConfig`, `CMDBM_Database`, `CMDBM_DatabaseCreate` |
| Sessions: running mapped statements | `CMDBM_Session` — `Execute`, `GetObject`, `GetRow`, `GetRowSet`, `ForEachRow` and the transaction methods |
| The context | `CMDBM_Context`, `CMDBM_ContextCreate` |

Two things the reference deliberately does not document, because neither is C
API: the JSON configuration file and the XML mapper syntax. Both are in the
[project README](../README.md), sections 6 and 7.

For working code, [`test/`](../test) has runnable programs, including a
complete DBMS module ([`mockdb.c`](../test/mockdb.c)) written against
`CMDBM_ModuleInterface` — the shortest way to see what a module has to do.

## Keeping it honest

The configuration leaves `EXTRACT_ALL` off and turns every documentation
warning on, so an undocumented entity is reported rather than published as a
blank page:

```
WARN_IF_UNDOCUMENTED   = YES
WARN_IF_INCOMPLETE_DOC = YES
WARN_NO_PARAMDOC       = YES
```

Warnings go to `doc/doxygen.log`. **It should be empty.** If a run leaves
anything in it, that is a doc comment to fix — a missing `@param`, a `@param`
naming an argument that no longer exists, a missing `@return` — not a message
to ignore.

One thing worth knowing when editing the header: `@typedef` and `@struct` are
Doxygen commands that take a *declaration*, not a name and a description. A
comment sitting directly above the entity already documents it, so a plain
`@brief` is what belongs there; writing `@typedef CMDBM_Foo Some description`
creates a phantom symbol and leaves the real one undocumented.
