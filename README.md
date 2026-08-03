# libcmdbm

A MyBatis-like database mapping library for C.

[![Build and Test](https://github.com/xcomart/libcmdbm/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/xcomart/libcmdbm/actions/workflows/build-and-test.yml)
[![Release](https://img.shields.io/github/v/release/xcomart/libcmdbm?sort=semver)](https://github.com/xcomart/libcmdbm/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Language: C99](https://img.shields.io/badge/language-C99-blue.svg)](#4-building)
[![Databases](https://img.shields.io/badge/databases-MariaDB%20%7C%20MySQL%20%7C%20PostgreSQL%20%7C%20SQLite%20%7C%20Oracle%20%7C%20ODBC-blue.svg)](#63-connection-parameters-per-module)
[![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20macOS-lightgrey.svg)](#4-building)
[![API reference](https://img.shields.io/badge/docs-API%20reference-brightgreen.svg)](https://xcomart.github.io/libcmdbm/api/)

📖 **[API reference](https://xcomart.github.io/libcmdbm/api/)** — every type and
method of `libcmdbm.h`, grouped by subject. This page is the guided tour; that
one is the lookup.

* [1. About](#1-about)
* [2. Features](#2-features)
* [3. How it works](#3-how-it-works)
* [4. Building](#4-building)
* [5. Quick start](#5-quick-start)
* [6. Configuration reference](#6-configuration-reference)
* [7. Mapper file reference](#7-mapper-file-reference)
* [8. API reference](#8-api-reference)
* [9. Writing a custom DBMS module](#9-writing-a-custom-dbms-module)
* [10. Logging](#10-logging)
* [11. Known issues and limitations](#11-known-issues-and-limitations)
* [12. Repository layout](#12-repository-layout)
* [13. License](#13-license)

## 1. About

Writing a database application in plain C means a lot of avoidable work:

* changing a query means recompiling the application,
* migrating to another DBMS means rewriting most of the data access layer,
* each DBMS has its own client API, connection handling and bind syntax.

libcmdbm removes all three. Queries live in XML mapper files that are parsed at
runtime (and reloaded when they change on disk), parameters and result rows are
plain JSON objects, and every supported DBMS is reached through the exact same
session API.

libcmdbm is written in C99, has an object-like API (structures of function
pointers, invoked through the `CMCall` macro from
[libcmutils](https://github.com/xcomart/libcmutils)) and runs anywhere
libcmutils runs.

Developed and maintained by Dennis Soungjin Park &lt;xcomart@gmail.com&gt;.

## 2. Features

* **Runtime SQL** — queries live in XML mapper files, so SQL can be changed
  without recompiling or restarting the application.
* **Dynamic SQL tags** — MyBatis style `<if>`, `<choose>`/`<when>`/
  `<otherwise>`, `<where>`, `<set>`, `<trim>`, `<foreach>`, `<include>`,
  `<bind>` and `<selectKey>`.
* **Parameter binding** — `#{name}` becomes a real bind variable in the
  DBMS-native syntax (`?`, `$1::int8`, `?1`, `:1`), `${name}` is substituted into the
  SQL text, `#{name, mode=out}` registers an OUT parameter.
* **JSON in, JSON out** — parameters are a `CMUTIL_JsonObject`, result rows come
  back as `CMUTIL_JsonObject` / `CMUTIL_JsonArray` with column types mapped to
  JSON long/double/boolean/string/null. `resultType="value"` turns a result set
  into a plain list of values.
* **Connection pooling** — per-datasource initial/max count, ping interval and
  test-on-borrow, backed by `CMUTIL_Pool`.
* **Hot reload** — mapper files and mapper directories are polled on a timer and
  reloaded when their modification time changes; queries in use are protected by
  a read/write lock.
* **Multiple datasources** — one context can hold any number of datasources of
  different DBMS types, each addressed by its source id.
* **Pluggable DBMS modules** — MariaDB/MySQL, PostgreSQL, SQLite, Oracle (OCI)
  and ODBC ship in the box; any other database can be added by implementing a single
  `CMDBM_ModuleInterface` structure and registering it.

## 3. How it works

```
   CMDBM_Context ─┬─ CMDBM_Database "sales"  ─┬─ mapper files (XML)  ← hot reload
                  │                           ├─ connection pool
                  │                           └─ CMDBM_ModuleInterface (PGSQL)
                  └─ CMDBM_Database "legacy" ─┬─ mapper files (XML)
                                              ├─ connection pool
                                              └─ CMDBM_ModuleInterface (ORACLE)

   CMDBM_Session (one per thread / per unit of work)
        └─ borrows one connection per datasource it touches
```

A call such as `CMCall(sess, GetRow, "sales", "user.selectUser", params)` does:

1. look up the datasource `sales` in the context,
2. take a read lock on its query repository and fetch the parsed XML node for
   `user.selectUser`,
3. borrow a pooled connection (kept in the session until it is closed),
4. walk the XML tree, evaluating the dynamic tags against `params`, producing a
   final SQL string plus an ordered array of bind values,
5. hand SQL + binds to the DBMS module, convert the result to JSON,
6. run any `<selectKey order="AFTER">` fragments,
7. release the query lock and temporary buffers.

## 4. Building

### Prerequisites

* CMake 3.10 or later and a C99 compiler
* [libcmutils](https://github.com/xcomart/libcmutils) — bundled as a git
  submodule; it requires zlib and OpenSSL development packages
* Client libraries for the DBMS modules you enable:

| Option | Default | Needs |
|---|---|---|
| `SUPPORT_MARIA` | `ON` | `libmariadb` (pkg-config) |
| `SUPPORT_PGSQL` | `ON` | `libpq` (pkg-config) |
| `SUPPORT_SQLITE` | `ON` | `sqlite3` (pkg-config) |
| `SUPPORT_ODBC` | `ON` | `odbc` / unixODBC (pkg-config) |
| `SUPPORT_MYSQL` | `OFF` | `mysqlclient` (pkg-config) |
| `SUPPORT_ORACLE` | `OFF` | Oracle Instant Client + SDK, `ORACLE_HOME` set |

`SUPPORT_MARIA` and `SUPPORT_MYSQL` share one module; enabling both registers it
under both the `MARIA` and `MYSQL` keys.

### Unix / Linux / macOS

```sh
git clone https://github.com/xcomart/libcmdbm.git
cd libcmdbm
git submodule update --init --recursive

cmake -B build \
    -DSUPPORT_MARIA=ON \
    -DSUPPORT_PGSQL=ON \
    -DSUPPORT_SQLITE=ON \
    -DSUPPORT_ODBC=ON \
    -DSUPPORT_MYSQL=OFF \
    -DSUPPORT_ORACLE=OFF
cmake --build build
sudo cmake --install build      # lib/libcmdbm.so, lib/libcmdbm.a, include/libcmdbm.h
```

On macOS the Homebrew packages (`brew install mariadb-connector-c libpq
unixodbc openssl`) are located through `pkg-config`; the build prepends the
matching `$HOMEBREW_PREFIX/opt/*/lib/pkgconfig` paths automatically.

If the DB development packages are not installed system-wide, point pkg-config
at your own prefix:

```sh
PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig \
cmake -B build -DCMAKE_PREFIX_PATH=$PREFIX ...
```

Both a shared (`libcmdbm.so`) and a static (`libcmdbm.a`) library are produced.
The shared library records its DBMS client libraries as dependencies, so
applications only need `-lcmdbm -lcmutils`.

### Running the tests

Tests are built with `-DBUILD_TESTS=ON` (the default) and are named
`cmdbm_*`, so they can be run without the tests of the libcmutils submodule:

```sh
ctest --test-dir build -R cmdbm_ --output-on-failure
```

| Test | Covers |
|---|---|
| `cmdbm_config_test` | configuration in both forms: type lookup, which keys reach the module, mapper loading, logging flags |
| `cmdbm_mapper_test` | `resultType`, `fetchSize` and cursor iteration |
| `cmdbm_libinit_test` | `LibraryInit` / `LibraryClear` reference counting |
| `cmdbm_pool_test` | which statement validates a pooled connection, and when it is run |
| `cmdbm_sqlite_test` | the SQLite module end to end against a real database file |
| `cmdbm_maria_test` | the MySQL/MariaDB module against a server |
| `cmdbm_pgsql_test` | the PostgreSQL module against a server |

The first four run against a mock DBMS module ([test/mockdb.c](test/mockdb.c))
and need no database at all; the SQLite test is skipped when `SUPPORT_SQLITE`
is off.

The two integration tests need a server to talk to.
[test/docker/compose.yml](test/docker/compose.yml) brings up a MariaDB 11 on
host port 13306 and a PostgreSQL 17 on host port 15432, both with the database
`cmdbmtest` and the account `cmdbm`/`cmdbm`;
[test/docker/env.sh](test/docker/env.sh) exports the `CMDBM_TEST_MARIA_*` and
`CMDBM_TEST_PGSQL_*` variables the tests read:

```sh
docker compose -f test/docker/compose.yml up -d
. test/docker/env.sh
ctest --test-dir build -R cmdbm_ --output-on-failure
docker compose -f test/docker/compose.yml down -v
```

A test whose variables are not set exits with 77, which ctest reports as a
skip, so the suite is green on a checkout without docker as well — 7 passed
with the servers up, 5 passed and 2 skipped without them.

Both integration tests cover the same ground: bind variables of every JSON
type, `GetObject` / `GetRow` / `GetRowSet`, `resultType="value"`, the `<where>`
/ `<if>` / `<foreach>` tags, transaction commit and rollback, cursor iteration
with a `fetchSize`, `<selectKey>` (a sequence read `BEFORE` on PostgreSQL, an
auto-increment read `AFTER` on MariaDB) and OUT parameters.

### Windows

Under construction.

## 5. Quick start

### 5.1 Configuration — `cmdbm_config.json`

```json
{
    "databases": [
        {
            "type": "PGSQL",
            "id": "sales",
            "charset": "utf-8",
            "pool": { "confRef": "basePoolConfig" },
            "mappers": [
                { "type": "mapperSet", "basePath": "./mappers",
                  "filePattern": "*.xml", "recursive": true }
            ],
            "params": {
                "host": "127.0.0.1",
                "port": "5432",
                "dbname": "salesdb",
                "user": "scott",
                "password": "tiger"
            }
        }
    ],
    "poolConfigurations": [
        {
            "id": "basePoolConfig",
            "pingInterval": 30,
            "initCount": 5,
            "maxCount": 100,
            "testSql": "select 1"
        }
    ]
}
```

### 5.2 Mapper — `mappers/user.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<mapper namespace="user">

    <sql id="userColumns">
        user_id, user_name, age, reg_date
    </sql>

    <select id="selectUser">
        select <include refid="userColumns"/>
          from t_user
        <where>
            <if test="userId != null">     and user_id = #{userId}   </if>
            <if test="minAge != null">     and age &gt;= #{minAge}   </if>
            <if test="name != null">       and user_name like #{name}</if>
        </where>
        order by ${orderBy}
    </select>

    <insert id="insertUser">
        <selectKey keyProperty="userId" order="BEFORE">
            select nextval('seq_user')
        </selectKey>
        insert into t_user (user_id, user_name, age)
        values (#{userId}, #{userName}, #{age})
    </insert>

    <update id="updateUser">
        update t_user
        <set>
            <if test="userName != null"> user_name = #{userName}, </if>
            <if test="age != null">      age = #{age},            </if>
        </set>
        where user_id = #{userId}
    </update>

</mapper>
```

Every query is addressed as `namespace.id`, e.g. `user.selectUser`.

### 5.3 Application

```c
#include <libcmdbm.h>

int main(void)
{
    CMDBM_Context *ctx;
    CMDBM_Session *sess;
    CMUTIL_JsonObject *params, *row;

    CMDBM_Init();                       /* required before anything else */

    ctx = CMDBM_ContextCreate("cmdbm_config.json", "UTF-8", NULL);
    if (ctx == NULL) return 1;

    sess = CMCall(ctx, GetSession);

    params = CMUTIL_JsonObjectCreate();
    CMCall(params, PutLong, "userId", 100);
    CMCall(params, PutString, "orderBy", "user_name");

    row = CMCall(sess, GetRow, "sales", "user.selectUser", params);
    if (row) {
        printf("name=%s\n", CMCall(row, GetCString, "user_name"));
        CMUTIL_JsonDestroy(row);        /* results are owned by the caller */
    }

    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);                /* returns connections to the pool */
    CMCall(ctx, Destroy);
    CMDBM_Clear();
    return 0;
}
```

Compile and link:

```sh
cc app.c -lcmdbm -lcmutils -o app
```

### 5.4 Building a datasource in code

A context can also be created empty (`CMDBM_ContextCreate(NULL, "UTF-8", NULL)`)
and populated programmatically:

```c
CMDBM_PoolConfig pool = {
    30,         /* pingterm     - idle test every 30s    */
    CMTrue,     /* pingtest                              */
    CMTrue,     /* testonborrow                          */
    2,          /* initcnt                               */
    10,         /* maxcnt                                */
    NULL        /* testsql      - NULL: the module's own */
};
CMUTIL_JsonObject *cparm = CMUTIL_JsonObjectCreate();
CMDBM_Database *db;

CMCall(cparm, PutString, "host", "127.0.0.1");
CMCall(cparm, PutString, "dbname", "salesdb");
CMCall(cparm, PutString, "user", "scott");
CMCall(cparm, PutString, "password", "tiger");

db = CMDBM_DatabaseCreate("sales", "PGSQL", "utf-8", &pool, cparm);
CMCall(db, AddMapperSet, "./mappers", "*.xml", CMTrue);
CMCall(db, SetMonitor, 10);             /* reload check every 10 seconds */
CMCall(ctx, AddDatabase, db);           /* context owns db from now on   */

CMUTIL_JsonDestroy(cparm);              /* params are cloned internally  */
```

`CMDBM_DatabaseCreate` clones both the pool config and the parameter object, so
the caller keeps ownership of what it passed in. `AddDatabase` initializes the
module, opens the initial pool connections, performs the first mapper load and
schedules the reload task; from that point the context owns and destroys the
database.

## 6. Configuration reference

The configuration file read by `CMDBM_ContextCreate` may be written in JSON or
in XML. The form is decided by the content, not by the file name: a file whose
first non-blank character is `<` is parsed as XML, anything else as JSON. The
XML form is converted into exactly the JSON structure described below and then
handed to the same parser, so both forms mean the same thing — see
[6.6](#66-the-xml-form).

All object keys are case-insensitive (they are lowercased before use), so
`filePattern`, `filepattern` and `FILEPATTERN` are the same key.
`data/cmdbm_config.json` and `data/cmdbm_config.xml` are two spellings of one
sample configuration, and `data/cmdbm_config.dtd` describes the XML form.

### 6.1 Top level

| Key | Type | Description |
|---|---|---|
| `databases` | array | one entry per datasource, required |
| `poolConfigurations` | array | named pool presets, referenced by `pool.confRef` |

### 6.2 Datasource entry

| Key | Type | Description |
|---|---|---|
| `type` | string | DBMS key, case-insensitive: `ODBC`, `ORACLE`, `MYSQL`, `MARIA`, `PGSQL`, `SQLITE`, or a key registered with `CMDBM_RegisterDBMS` |
| `id` | string | source id used as the `dbid` argument of every session call, required |
| `charset` | string | database character set, defaults to `utf-8` |
| `pool` | object | pool settings, see below |
| `mappers` | array | mapper entries, see below |
| `monitorInterval` | number | seconds between two mapper rescans, defaults to 30 |
| `params` | object | connection parameters passed to the DBMS module |

Every scalar (non-object, non-array) key of the datasource entry is *also*
copied into the parameter object handed to the module, so `"host": "..."` may be
written either at the top level of the entry or inside `params`. The keys listed
in the table above (`type`, `id`, `charset`, `monitorInterval`, `pool`,
`mappers`, `params`) describe the datasource itself and are never forwarded as
connection parameters.

### 6.3 Connection parameters per module

| Module | Keys |
|---|---|
| `MYSQL` / `MARIA` | `host`, `user`, `password`, `database`, `port` (default 3306) |
| `PGSQL` | any libpq keyword — `host`, `port`, `dbname`, `user`, `password`, `sslmode`, … ; `database` is accepted as an alias of `dbname`, and `client_encoding` is set automatically from the program charset |
| `SQLITE` | `file` — a path or `:memory:`; optional `serialize` (default `true`, gives every pooled connection its own mutex) |
| `ORACLE` | `tnsname`, `user`, `password` |
| `ODBC` | `dsn`, `user`, `password` (connection string is built as `DSN=…;Uid=…;Pwd=…;`) |

### 6.4 Pool settings

| Key | Default | Description |
|---|---|---|
| `confRef` | — | id of an entry in `poolConfigurations` to inherit from |
| `initCount` | 5 | connections opened at start-up |
| `maxCount` | 20 (100 in the sample preset) | hard limit of pooled connections |
| `pingInterval` | 30 | seconds between two rounds of the idle-connection test |
| `pingTest` | `true` | whether idle connections are tested at all |
| `testOnBorrow` | `true` | whether a connection is tested when a session takes it |
| `testSql` | the statement of the module | query used for those tests |

A checkout waits at most 5 seconds for a free connection before failing.

A connection which fails its test is closed and replaced, which is what keeps
a pool usable across a database restart. `testOnBorrow` is the safe setting —
a session never gets a dead connection — at one round trip per checkout;
turning it off and leaving `pingTest` on moves the cost to the background
task. With both off, connections are handed out without ever being tested.

Leaving `testSql` out is usually right: each module brings the statement its
database needs, `select 1 from dual` on Oracle and `select 1` elsewhere.

### 6.5 Mapper entries

```json
"mappers": [
    { "type": "mapper",    "filePath": "./mappers/user.xml" },
    { "type": "mapperSet", "basePath": "./mappers", "filePattern": "*.xml",
      "recursive": true }
]
```

| Key | Applies to | Description |
|---|---|---|
| `type` | both | `mapper` (single file) or `mapperSet` (directory scan) |
| `filePath` | `mapper` | path to one XML mapper file |
| `basePath` | `mapperSet` | directory to scan |
| `filePattern` | `mapperSet` | glob pattern (`*`, `?`, `[a-z]`, `**/`) |
| `recursive` | `mapperSet` | descend into subdirectories |

Mapper files and mapper sets are re-checked every 30 seconds by default. The
interval is set in the configuration with the `monitorInterval` key of the
datasource entry (`<Mappers monitorInterval="10">` in the XML form), or in code
with `CMCall(db, SetMonitor, seconds)` before `AddDatabase`. Changed files are
re-parsed and swapped in, deleted files are dropped, and new files matching a
mapper set are picked up.

### 6.6 The XML form

The same configuration may be written as XML; `CMDBM_ContextCreate` recognizes
it by its first non-blank character being `<`. The document is converted into
the JSON structure of the sections above, so every key described there exists
in both forms. The rules of the conversion:

| XML | Becomes |
|---|---|
| `<Configuration>` | the root object |
| `<Databases>` | the `databases` array |
| a child of `<Databases>` | one datasource entry — **the tag name is its `type`**, so `<PgSql id="…">` is `"type": "PgSql"`. Any key registered with `CMDBM_RegisterDBMS` may be spelled as a tag |
| an attribute of a datasource tag | a key of the entry: `id="sales"` is `"id": "sales"` |
| a scalar child tag | the same: `<Host>127.0.0.1</Host>` is `"host": "127.0.0.1"`. When a setting is given both ways the child tag wins |
| `<Param key="k" value="v"/>` or `<Param key="k">v</Param>` | `params.k`; the `value` attribute wins over the text |
| `<Pool confRef="…" …/>` | the `pool` object, from its attributes and scalar child tags |
| `<Mappers monitorInterval="10">` | the `mappers` array; `monitorInterval` belongs to the datasource, not to a mapper |
| `<Mapper file="p"/>` | `{ "type": "mapper", "filePath": "p" }` — the text content is used when there is no attribute |
| `<MapperSet basePath="" filePattern="" recursive=""/>` | `{ "type": "mapperSet", … }` |
| `<Logging>` with `<QueryId show="…"/>`, `<Query show="…"/>`, `<Result show="…"/>` | the `logging` object, see [6.7](#67-logging-section) |
| `<PoolConfigurations>` with `<PoolConfig id="…" …/>` | the `poolConfigurations` array |

Tag and attribute names are matched case-insensitively, like the JSON keys. An
unknown tag or attribute is skipped with a warning rather than rejected, so a
configuration carrying settings of a newer version still loads.

The datasource of [5.1](#51-configuration--cmdbm_configjson) written as XML:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Configuration SYSTEM "cmdbm_config.dtd">
<Configuration>
    <Databases>
        <PgSql id="sales" charset="utf-8">
            <Host>127.0.0.1</Host>
            <Port>5432</Port>
            <Database>salesdb</Database>
            <User>scott</User>
            <Password>tiger</Password>
            <Pool confRef="basePoolConfig" />
            <Mappers monitorInterval="30">
                <MapperSet basePath="./mappers" filePattern="*.xml"
                        recursive="true" />
            </Mappers>
        </PgSql>
    </Databases>
    <PoolConfigurations>
        <PoolConfig id="basePoolConfig" pingInterval="30">
            <InitCount>5</InitCount>
            <MaxCount>100</MaxCount>
            <TestSql>select 1</TestSql>
        </PoolConfig>
    </PoolConfigurations>
</Configuration>
```

`data/cmdbm_config.xml` is the full sample, one datasource per built-in module,
and validates against `data/cmdbm_config.dtd`:

```sh
xmllint --valid --noout data/cmdbm_config.xml
```

A DTD cannot express that the tag name of a datasource is its DBMS type, so the
DTD lists the built-in types only — a configuration using a module registered
with `CMDBM_RegisterDBMS` will not validate against it although it loads. The
`<!DOCTYPE>` declaration is optional; libcmdbm does not validate.

### 6.7 Logging section

The optional top-level `logging` section chooses what a session writes to the
log. It is the counterpart of [10. Logging](#10-logging): that section is about
configuring the libcmutils loggers, this one about what libcmdbm gives them.

```json
"logging": {
    "queryId": true,
    "query": true,
    "result": false
}
```

```xml
<Logging>
    <QueryId show="true" />
    <Query show="true" />
    <Result show="false" />
</Logging>
```

| Key | Default | What it adds |
|---|---|---|
| `queryId` | `true` | the `dbid.sqlid` of the statement about to run |
| `query` | `true` | the SQL text actually sent to the database |
| `result` | `false` | what the call returned |

Values are a JSON boolean or the strings `"true"` / `"false"`; the keys are
case-insensitive like every other configuration key. The defaults above apply
when the section is absent, and to a context created without a configuration
file at all.

Everything the section controls is written at **DEBUG** level to the
`cmdbm.session` logger, so it is visible only when that logger is set to DEBUG.
`queryId` and `query` together decide the line logged just before a statement
runs — both on gives `dbid.sqlid - SQL`, `queryId` alone the id, `query` alone
the SQL text, both off nothing at all.

`result` adds a second line after the call: the affected row count for
`Execute`, the result as JSON for `GetObject`, `GetRow` and `GetRowSet`, and
one line per row for `ForEachRow`. It is off by default because a result set
can be large. A call which failed does not log a result — it has already
logged its error.

## 7. Mapper file reference

A mapper file is an XML document whose root is `<mapper namespace="...">`. Its
children are `<sql>`, `<select>`, `<insert>`, `<update>` and `<delete>`; each
needs an `id` that does **not** contain a dot. The full query id is
`namespace.id`. `data/cmdbm_sqlmap.dtd` is provided for editor validation.

### 7.1 Parameter expressions

| Syntax | Meaning |
|---|---|
| `#{name}` | bind variable — emits the DBMS-native placeholder (`?` for MySQL/MariaDB/ODBC, `$n::type` for PostgreSQL, `?n` for SQLite, `:n` for Oracle) and appends `params["name"]` to the bind list |
| `#{name, mode=out}` | OUT parameter — emits a placeholder as above, and the value the procedure produced is written back into `params["name"]` |
| `${name}` | literal substitution of `params["name"]` into the SQL text; use only for identifiers you control (table names, sort columns) — it is *not* escaped |

Bind values keep their JSON type: long, double, boolean, string or null. A
missing key for `#{}` fails the query build; a missing key for `#{…,mode=out}`
is created as a string placeholder.

How an OUT parameter is read back is up to the module, and so are its limits:

| Module | OUT parameters |
|---|---|
| `MYSQL` / `MARIA` | supported. The server returns the OUT and INOUT values as a result set of its own, which only a statement run through `Execute` reads — write the `CALL` as `<insert>`, `<update>` or `<delete>`. A `<select>` cannot read them and says so in the log |
| `PGSQL` | supported. PostgreSQL has no OUT binding: `CALL procedure(…)` hands the OUT and INOUT values back as a single result row, whose columns are written into the OUT parameters in declaration order |
| `ORACLE` / `ODBC` | the modules bind OUT parameters, but that code has never been exercised — see [11. Known issues](#11-known-issues-and-limitations) |
| `SQLITE` | not applicable — SQLite has no stored procedures |

```xml
<!-- both on MariaDB and on PostgreSQL: a CALL run through Execute -->
<update id="callSum">
    call p_sum(#{a}, #{b}, #{total, mode=out}, #{label, mode=out})
</update>
```

After `CMCall(sess, Execute, "sales", "user.callSum", params)` the keys `total`
and `label` of `params` hold what the procedure produced.

### 7.2 Tags

**`<include refid="..."/>`** — inlines a `<sql>` fragment. An id without a dot is
resolved inside the current namespace.

**`<if test="...">`** / **`<when test="...">`** — includes its body when the test
passes. The test language is deliberately small:

* comparisons: `=`, `==`, `!=`, `<>`, `>`, `<`, `>=`, `<=`
* combinators: `and`, `or`, and parentheses for grouping
* operands: a parameter name, a quoted literal (`'abc'`, `"abc"`), a number, or
  the keyword `null`
* if the right-hand operand looks numeric the comparison is numeric (doubles,
  compared with a 1e-7 tolerance), otherwise it is a string comparison
* `>` `<` `>=` `<=` are always numeric
* there is no operator precedence — terms are evaluated left to right with
  short-circuiting, so use parentheses when mixing `and` and `or`

```xml
<if test="status = 'ACTIVE' and (age >= 20 or vip = 'Y')"> ... </if>
```

Remember to escape `<` and `&` as `&lt;` / `&amp;` in XML attribute values.

**`<choose>` / `<when>` / `<otherwise>`** — first matching `<when>` wins;
`<otherwise>` is used when none matched.

**`<where>`** — wraps its body in `WHERE ` and strips a leading or trailing
`AND`/`OR`. Emits nothing when the body is empty.

**`<set>`** — wraps its body in `SET ` and strips a leading or trailing comma.

**`<trim prefix="" prefixOverrides="" suffix="" suffixOverrides="">`** — the
generic form behind `<where>` and `<set>`. `*Overrides` takes a `|`-separated
list of alternatives.

**`<foreach collection="items" item="it" index="i" open="(" close=")"
separator=",">`** — iterates a JSON array in the parameters, exposing the
current element as `params[item]` and the 0-based position as `params[index]`
while the body is built.

```xml
where user_id in
<foreach collection="ids" item="id" open="(" close=")" separator=",">
    #{id}
</foreach>
```

**`<bind name="x" value="10" type="int"/>`** — sets a parameter while building.
`type` is one of `string` (default), `int`/`long`, `float`/`double`.

**`<selectKey keyProperty="userId" order="BEFORE|AFTER">`** — runs its body as a
separate single-value query and stores the result in `params[keyProperty]`.
`BEFORE` (typical for sequences) runs before the main statement, `AFTER`
(default, typical for identity/auto-increment) runs after it.

### 7.3 Select attributes

`<select>` takes two optional attributes:

| Attribute | Values | Effect |
|---|---|---|
| `resultType` | `map` (default), `value` | shape of a result row |
| `fetchSize` | positive integer | rows fetched per round trip during cursor iteration |

**`resultType="value"`** reduces every row to the value of its first column, so
`GetRowSet` returns an array of JSON values instead of an array of row objects:

```xml
<select id="selectUserIds" resultType="value">
    select user_id from t_user order by user_id
</select>
```

```c
CMUTIL_JsonArray *ids = CMCall(sess, GetRowSet, "sales", "user.selectUserIds", params);
CMUTIL_JsonValue *id = (CMUTIL_JsonValue*)CMCall(ids, Get, 0);
printf("%ld\n", CMCall(id, GetLong));
```

A row without any column contributes a JSON null. The attribute has no meaning
for `GetObject` (already a single value) and is ignored — with a warning for
`ForEachRow`, whose callback always receives a row object — by the other calls.

**`fetchSize`** applies to cursor iteration (`ForEachRow`) and is a hint: what a
module does with it depends on what its client library offers.

| Module | Effect of `fetchSize` |
|---|---|
| `PGSQL` | switches libpq to single row mode, so the result set is streamed instead of being materialized in client memory. libpq has no batch size below PostgreSQL 17, so the value itself is not used |
| `MYSQL` / `MARIA` | opens a read-only server side cursor and sets it as the prefetch row count |
| `ORACLE` | sets `OCI_ATTR_PREFETCH_ROWS` of the statement |
| `SQLITE` | ignored: an embedded engine has no round trip to batch, `sqlite3_step` already produces one row at a time |
| `ODBC` | ignored: block fetching would need array bound result columns |

It is ignored (with a warning) when the statement also carries a
`<selectKey order="AFTER">`, because that statement has to run on the same
connection while the cursor is still open.

### 7.4 Example putting it together

```xml
<select id="searchOrders">
    select o.order_id, o.amount, c.name
      from t_order o join t_customer c on c.id = o.customer_id
    <where>
        <if test="from != null">    and o.order_date &gt;= #{from} </if>
        <if test="to != null">      and o.order_date &lt;= #{to}   </if>
        <choose>
            <when test="state = 'OPEN'">   and o.state in ('N','P') </when>
            <when test="state = 'CLOSED'"> and o.state = 'C'        </when>
            <otherwise>                    and o.state &lt;&gt; 'X' </otherwise>
        </choose>
        <if test="ids != null">
            and o.order_id in
            <foreach collection="ids" item="id" open="(" close=")" separator=",">
                #{id}
            </foreach>
        </if>
    </where>
    order by ${sortColumn} desc
</select>
```

## 8. API reference

All types are declared in `src/libcmdbm.h`. Methods are invoked with libcmutils'
`CMCall(obj, Method, args...)` macro, which expands to
`obj->Method(obj, args...)`. Note that `CMCall` cannot be nested — assign
intermediate results to local variables.

The section below is the summary. The header carries a doc comment on every
declaration, published as the
**[API reference](https://xcomart.github.io/libcmdbm/api/)** — the same pages
are built locally with `cmake --build build --target cmdbm_docs`, see
[doc/README.md](doc/README.md).

### 8.1 Library lifecycle

```c
void        CMDBM_Init(void);       /* initializes libcmutils + libcmdbm */
void        CMDBM_Clear(void);      /* releases global state             */
const char *CMDBM_GetLibVersion(void);
```

`CMDBM_Init()` must be called before any other libcmdbm function, and
`CMDBM_Clear()` after everything else has been destroyed.

### 8.2 Context

```c
CMDBM_Context *CMDBM_ContextCreate(const char *confjson,      /* optional */
                                   const char *progcharset,   /* optional */
                                   CMUTIL_Timer *timer);      /* optional */
```

* `confjson` — path to the JSON configuration, or `NULL` for an empty context.
* `progcharset` — character set of strings your program passes and receives;
  defaults to `UTF-8`. It is what the modules convert to and from.
* `timer` — an existing `CMUTIL_Timer` to schedule pool and mapper maintenance
  on; when `NULL`, the context creates and owns one.

Returns `NULL` if the configuration could not be loaded.

| Method | Description |
|---|---|
| `AddDatabase(db)` | initialize and take ownership of a datasource |
| `GetSession()` | create a new session |
| `Destroy()` | destroy every datasource, then the internal timer |

### 8.3 Database

```c
CMDBM_Database *CMDBM_DatabaseCreate(const char *sourceid,
                                     const char *dbmskey,
                                     const char *dbcharset,
                                     CMDBM_PoolConfig *poolconf,
                                     CMUTIL_JsonObject *params);
```

| Method | Description |
|---|---|
| `AddMapper(file)` | add or replace one mapper file |
| `AddMapperSet(basepath, pattern, recursive)` | add a watched directory |
| `SetMonitor(interval)` | reload check interval in seconds (default 30) |
| `Destroy()` | release the datasource (normally done by the context) |

### 8.4 Session

A session is a short-lived, single-threaded unit of work. It lazily borrows one
connection per datasource it touches and returns them all on `Close`. Create one
session per thread — sessions are not thread-safe, the context is.

| Method | Returns | Description |
|---|---|---|
| `Execute(dbid, sqlid, params)` | `int` | affected row count, `-1` on failure |
| `GetObject(dbid, sqlid, params)` | `CMUTIL_JsonValue*` | first column of the first row, `NULL` on failure |
| `GetRow(dbid, sqlid, params)` | `CMUTIL_JsonObject*` | first row as column→value, `NULL` on failure |
| `GetRowSet(dbid, sqlid, params)` | `CMUTIL_JsonArray*` | all rows, or all first-column values with `resultType="value"`; `NULL` on failure |
| `ForEachRow(dbid, sqlid, params, udata, rowcb)` | `CMBool` | cursor iteration, honours `fetchSize` |
| `BeginTransaction()` / `Commit()` / `Rollback()` / `EndTransaction()` | | transaction control |
| `Close()` | | return connections to their pools and free the session |

Ownership rules:

* `params` always stays owned by the caller — destroy it yourself.
* Values returned by `GetObject`, `GetRow` and `GetRowSet` are owned by the
  caller — release them with `CMUTIL_JsonDestroy`.
* The row handed to a `ForEachRow` callback is destroyed by the library right
  after the callback returns; copy anything you need to keep.

Cursor iteration:

```c
static CMBool print_row(CMUTIL_JsonObject *row, uint32_t rownum, void *udata)
{
    printf("%u: %s\n", rownum, CMCall(row, GetCString, "user_name"));
    return CMTrue;              /* return CMFalse to stop iterating */
}

CMCall(sess, ForEachRow, "sales", "user.selectAll", params, NULL, print_row);
```

Transactions:

```c
CMCall(sess, BeginTransaction);
if (CMCall(sess, Execute, "sales", "user.insertUser", params) >= 0) {
    CMCall(sess, Commit);
} else {
    CMCall(sess, Rollback);
}
CMCall(sess, EndTransaction);
```

`BeginTransaction` starts a transaction on every connection the session holds,
and any connection borrowed afterwards joins it automatically, so the call may
be made before the first statement. `Commit`, `Rollback` and `EndTransaction`
apply to all connections of the session — which makes a session spanning several
datasources a best-effort multi-database transaction, not a two-phase commit.
Closing a session while a transaction is still open rolls it back rather than
returning a dirty connection to the pool.

## 9. Writing a custom DBMS module

Any database can be plugged in by filling a `CMDBM_ModuleInterface` and
registering it before creating datasources of that type:

```c
static CMDBM_ModuleInterface my_module = {
    MyLibraryInit, MyLibraryClear, MyGetDBMSKey,
    MyInitialize,  MyCleanUp,      MyGetBindString, MyGetTestQuery,
    MyOpenConnection, MyCloseConnection,
    MyStartTransaction, MyEndTransaction,
    MyCommitTransaction, MyRollbackTransaction,
    MyGetOneValue, MyGetRow, MyGetList, MyExecute,
    MyOpenCursor, MyCloseCursor, MyCursorNextRow
};

CMDBM_Init();
CMDBM_RegisterDBMS("MYDB", &my_module);
```

The key parts of the contract:

* `GetDBMSKey()` names the client library the module drives. It is the identity
  used for library set-up, so a module registered under several keys (as the
  MySQL module is, under `MYSQL` and `MARIA`) must report one key.
* `LibraryInit()` runs once, before the first datasource of that library is
  created, and `LibraryClear()` once, after the last one has been destroyed —
  no matter how many datasources of that type, in how many contexts, exist in
  between. Creating a datasource again after the last one was destroyed
  initializes the library again, so leave `LibraryClear` empty when the client
  library cannot be re-initialized, or when the host application may be using
  it too. Both bundled cases are of that kind: the SQLite module does not call
  `sqlite3_shutdown` and the MySQL module does not call `mysql_library_end`.
* `Initialize(dbcs, prcs)` returns an opaque per-datasource context (database
  charset, program charset) that is passed back to every other callback;
  `CleanUp` releases it.
* `GetBindString(initres, index, buffer, vtype)` writes the placeholder for the
  0-based bind slot `index` into `buffer` — e.g. `?`, `:1`, `$1::int8`.
* `OpenConnection(initres, params)` receives the merged configuration parameters
  and returns an opaque connection handle; the pool calls `CloseConnection` on
  it eventually.
* The execution callbacks receive the built SQL (`CMUTIL_String`), the ordered
  bind array (`CMUTIL_JsonArray` of value nodes) and the OUT parameter map
  (`CMUTIL_JsonObject` keyed by bind index as a decimal string). Bind and OUT
  values are borrowed references — never destroy them.
* `GetOneValue` returns the first column of the first row, `GetRow` one JSON
  object, `GetList` an array of objects, `Execute` an affected row count, and
  the cursor triple streams rows one at a time.
* `OpenCursor` also receives the `fetchSize` of the statement, 0 when it is not
  given. It is a hint — a module whose client library cannot control the fetch
  size may ignore it.

The bundled modules in [modules/](modules/) are the reference implementations.

## 10. Logging

Logging goes through libcmutils. Without any configuration a default console
logger is installed on the first message.

The simplest way to control it needs no code at all: put a `cmutil_log.jsonc`
in the working directory, or point the `CMUTIL_LOG_CONFIG` environment variable
at your file. It is read when the first message is logged, so it also covers
what the library logs while starting up.

Configuring it explicitly is a matter of initialization order. The log system
lives in libcmutils and needs the memory system that `CMUTIL_Init` sets up, so
it cannot be configured before that — doing so crashes. `CMDBM_Init` calls
`CMUTIL_Init` for you:

```c
CMDBM_Init();                                     /* initializes libcmutils */
CMUTIL_LogSystemConfigureFomJson("mylog.jsonc");  /* installs itself */
```

`CMUTIL_Init` is reference counted, so an application which wants its
configuration in place before libcmdbm logs anything may take the first
reference itself, and give it back at the end:

```c
CMUTIL_Init(CMMemSystem);
CMUTIL_LogSystemConfigureFomJson("mylog.jsonc");
CMDBM_Init();
    ...
CMDBM_Clear();
CMUTIL_Clear();                                   /* the reference above */
```

The configuration call installs its result globally, so handing it back to
`CMUTIL_LogSystemSet` is a no-op — harmless since libcmutils 0.6.4, a crash in
earlier versions.

See `libcmutils/samples/cmutil_log.jsonc` for the format — note that a logger
refers to its appenders with `appenderRef`. libcmdbm uses these logger names:

| Logger | Content |
|---|---|
| `cmdbm.context` | configuration parsing |
| `cmdbm.database` | pool, mapper loading and reloading |
| `cmdbm.mapper` | mapper file parsing errors |
| `cmdbm.sqlbuild` | dynamic SQL construction |
| `cmdbm.session` | the final SQL of every call (DEBUG) and execution errors |
| `cmdbm.connection` | connection checkout/return (TRACE) |
| `cmdbm.module.*` | DBMS specific messages |

Set `cmdbm.session` to `DEBUG` to see each query id together with the SQL text
actually sent to the database. *What* that logger is given — the query id, the
SQL text, the result — is chosen in the configuration file instead, see
[6.7 Logging section](#67-logging-section).

## 11. Known issues and limitations

Current state of version 0.1.2 — worth knowing before you file a bug:

* **OUT parameters are unverified on Oracle and ODBC.** `#{…, mode=out}` works
  and is covered by the integration tests on MySQL/MariaDB and PostgreSQL. The
  Oracle and ODBC modules have carried OUT binding code for as long, but a bug
  in the mapper kept `mode=out` from ever reaching a module, so that code has
  never run; there was no server to try it against once the bug was fixed.
  Treat it as untested rather than as working.
* **MySQL/MariaDB cannot read OUT parameters from a `<select>`.** The values
  arrive as a separate result set which only `Execute` reads, so a `CALL` with
  OUT parameters has to be written as `<insert>`, `<update>` or `<delete>` —
  see [7.1](#71-parameter-expressions).

## 12. Repository layout

```
src/            core: context, database, session, connection, mapper, sqlbuild
modules/        DBMS modules: cmdbm_mysql.c, cmdbm_pgsql.c, cmdbm_sqlite.c,
                cmdbm_oracle.c, cmdbm_odbc.c
data/           sample configuration, sample sqlmap and DTDs
doc/            doxygen configuration for the API reference
test/           test suite, with a mock DBMS module, its mapper/config data
                and test/docker/ - the servers the integration tests need
libcmutils/     git submodule — base utility library (JSON, XML, pool, log, …)
.github/        CI workflow building and testing on Linux and macOS
CMakeLists.txt  build definition
VERSION         library version, read at configure time
```

## 13. License

MIT License — see [LICENSE](LICENSE).

Copyright (c) 2026 Dennis Soungjin Park &lt;xcomart@gmail.com&gt;
