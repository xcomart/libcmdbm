# libcmdbm

## 1. About

libcmdbm is a database wrapper library with object based approach.

To build database application in C, there are many complex jobs to do.

* If query has to be changed, application must be recompiled.
* If database migration is needed, almost whole program must be recreated.
* If there are multiple types of database, need to use different library API
for each database types.

This library is much like with MyBatis java library.

The libcmdbm library :

* has intuitive structure to use like OOP language
* runs on almost every platforms
* supports Oracle, MySQL/MariaDB, PostgreSQL and ODBC basically out of box,
    and any database can be added by creating interface module.
* offers same API with different database type.
* supports connecting to multiple databases.
* connection pooling implemented by default.
* uses dynamic query building by XML query files.

libcmdbm is developed and maintained by Dennis Soungjin Park <xcomart@gmail.com>.

## 2. License

The source code freely to use under **MIT License** - see [LICENSE file](LICENSE)

## 3. Features

* **XML query mapper** — queries live in XML files
  (see [data/cmdbm_sqlmap.xml](data/cmdbm_sqlmap.xml)), so SQL can be changed
  without recompiling the application.
* **Dynamic SQL tags** — MyBatis style tags are supported:
  `<if test="...">`, `<choose>`/`<when>`/`<otherwise>`, `<where>`, `<set>`,
  `<trim>`, `<foreach>`, `<include>`, `<selectKey>`, `<bind>`.
* **Parameter binding** — `#{name}` becomes a bind variable
  (out parameters via `#{name, mode=out}`), `${name}` is replaced literally.
* **JSON based parameters and results** — parameters are passed as
  `CMUTIL_JsonObject` and rows come back as JSON objects/arrays.
* **Connection pooling** — initial/max counts, ping test and test-on-borrow
  are configurable per datasource (see
  [data/cmdbm_config.json](data/cmdbm_config.json)).
* **Hot reload** — mapper files are monitored and reloaded automatically
  when changed.
* **Multiple databases** — a single context can hold several datasources of
  different DBMS types, each addressed by its source id.
* **Session API** — `Execute`, `GetObject`, `GetRow`, `GetRowSet`,
  `ForEachRow` (cursor based iteration) with explicit transaction control
  (`BeginTransaction`/`Commit`/`Rollback`).

## 4. Installation

### Prerequisites

* CMake 3.10 or later, C99 compiler
* [libcmutils](https://github.com/xcomart/libcmutils) — included as a git
  submodule (requires zlib and OpenSSL development packages)
* Client libraries for the databases you enable:
  * MariaDB: `libmariadb-dev` (or MySQL client library with `SUPPORT_MYSQL`)
  * PostgreSQL: `libpq-dev`
  * ODBC: `unixodbc-dev`
  * Oracle: Oracle Instant Client with SDK (`ORACLE_HOME` must be set)

### GNU (Unix / Linux) platforms

```sh
git clone https://github.com/xcomart/libcmdbm.git
cd libcmdbm
git submodule update --init --recursive

cmake -B build \
    -DSUPPORT_MARIA=ON \
    -DSUPPORT_PGSQL=ON \
    -DSUPPORT_ODBC=ON \
    -DSUPPORT_MYSQL=OFF \
    -DSUPPORT_ORACLE=OFF
cmake --build build
sudo cmake --install build
```

Each `SUPPORT_*` option toggles the corresponding database module.
Both shared (`libcmdbm.so`) and static (`libcmdbm.a`) libraries are built.

### macOS platforms

Install dependencies with Homebrew (`brew install mariadb-connector-c libpq
unixodbc openssl`), then build the same way as Linux — the build script
locates Homebrew packages via `pkg-config` automatically.

### Windows platforms (Visual Studio / MinGW)

under construction.

## 5. Examples

Configuration samples are provided in the [data/](data/) directory:

* [cmdbm_config.json](data/cmdbm_config.json) — datasource/pool configuration
* [cmdbm_sqlmap.xml](data/cmdbm_sqlmap.xml) — query mapper sample

```c
#include <libcmdbm.h>

int main(void)
{
    CMDBM_Context *ctx = CMDBM_ContextCreate(
            "cmdbm_config.json", "UTF-8", NULL);
    CMDBM_Session *sess = CMCall(ctx, GetSession);
    CMUTIL_JsonObject *params = CMUTIL_JsonObjectCreate();

    CMCall(params, PutLong, "userId", 100);
    CMUTIL_JsonObject *row = CMCall(sess, GetRow,
            "MyDatabase", "user.selectUser", params);
    if (row) {
        // use result row
        CMUTIL_JsonDestroy(row);
    }

    CMUTIL_JsonDestroy(params);
    CMCall(sess, Close);
    CMCall(ctx, Destroy);
    return 0;
}
```
