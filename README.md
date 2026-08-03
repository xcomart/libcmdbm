# libcmdbm

A MyBatis-like database mapping library for C.

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
  DBMS-native syntax (`?`, `$1::int8`, `:1`), `${name}` is substituted into the
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
    30,         /* pingterm     - ping interval, seconds */
    CMTrue,     /* pingtest                              */
    CMTrue,     /* testonborrow                          */
    2,          /* initcnt                               */
    10,         /* maxcnt                                */
    "select 1"  /* testsql                               */
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

The configuration file is JSON, read by `CMDBM_ContextCreate`. All object keys
are case-insensitive (they are lowercased before use), so `filePattern`,
`filepattern` and `FILEPATTERN` are the same key. `data/cmdbm_config.json` is a
sample, and `data/cmdbm_config.dtd` describes the equivalent (not yet
implemented) XML form.

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
| `params` | object | connection parameters passed to the DBMS module |

Every scalar (non-object, non-array) key of the datasource entry is *also*
copied into the parameter object handed to the module, so `"host": "..."` may be
written either at the top level of the entry or inside `params`. The keys listed
in the table above (`type`, `id`, `charset`, `pool`, `mappers`, `params`)
describe the datasource itself and are never forwarded as connection
parameters.

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
| `pingInterval` | 30 | seconds between idle-connection ping tests |
| `testSql` | `select 1` | query used for the ping/borrow test |

A checkout waits at most 5 seconds for a free connection before failing.

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

Mapper files and mapper sets are re-checked every 30 seconds by default; call
`CMCall(db, SetMonitor, seconds)` before `AddDatabase` to change the interval.
Changed files are re-parsed and swapped in, deleted files are dropped, and new
files matching a mapper set are picked up.

## 7. Mapper file reference

A mapper file is an XML document whose root is `<mapper namespace="...">`. Its
children are `<sql>`, `<select>`, `<insert>`, `<update>` and `<delete>`; each
needs an `id` that does **not** contain a dot. The full query id is
`namespace.id`. `data/cmdbm_sqlmap.dtd` is provided for editor validation.

### 7.1 Parameter expressions

| Syntax | Meaning |
|---|---|
| `#{name}` | bind variable — emits the DBMS-native placeholder (`?` for MySQL/MariaDB/ODBC, `$n::type` for PostgreSQL, `?n` for SQLite, `:n`
for Oracle) and appends `params["name"]` to the bind list |
| `#{name, mode=out}` | OUT parameter — bound as above, and the value produced by the DBMS is written back into `params["name"]` |
| `${name}` | literal substitution of `params["name"]` into the SQL text; use only for identifiers you control (table names, sort columns) — it is *not* escaped |

Bind values keep their JSON type: long, double, boolean, string or null. A
missing key for `#{}` fails the query build; a missing key for `#{…,mode=out}`
is created as a string placeholder.

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

Logging goes through libcmutils. Without configuration a default console logger
is installed; to control it, configure the log system before `CMDBM_Init`:

```c
CMUTIL_LogSystem *lsys = CMUTIL_LogSystemConfigureFomJson("cmutil_log.jsonc");
```

See `libcmutils/samples/cmutil_log.jsonc` for the format. libcmdbm uses these
logger names:

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
actually sent to the database.

## 11. Known issues and limitations

Current state of version 0.1.1 — worth knowing before you file a bug:

* **XML configuration is not implemented.** `data/cmdbm_config.xml` and
  `cmdbm_config.dtd` document the intended shape; only JSON is parsed today.
* **The `Logging` configuration section is not implemented.**
* **Oracle OUT parameters** are the only place `#{…, mode=out}` is fully
  meaningful; PostgreSQL has no OUT binding and returns procedure results as an
  ordinary result set.

## 12. Repository layout

```
src/            core: context, database, session, connection, mapper, sqlbuild
modules/        DBMS modules: cmdbm_mysql.c, cmdbm_pgsql.c, cmdbm_sqlite.c,
                cmdbm_oracle.c, cmdbm_odbc.c
data/           sample configuration, sample sqlmap and DTDs
libcmutils/     git submodule — base utility library (JSON, XML, pool, log, …)
CMakeLists.txt  build definition
VERSION         library version, read at configure time
```

## 13. License

MIT License — see [LICENSE](LICENSE).

Copyright (c) 2026 Dennis Soungjin Park &lt;xcomart@gmail.com&gt;
