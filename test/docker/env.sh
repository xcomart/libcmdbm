# Connection settings of the servers test/docker/compose.yml brings up.
# The integration tests of the client/server modules skip themselves when
# these variables are not set, so a checkout without docker still runs the
# whole suite green.
#
#   docker compose -f test/docker/compose.yml up -d
#   . test/docker/env.sh
#   ctest --test-dir build -R cmdbm_ --output-on-failure
#   docker compose -f test/docker/compose.yml down -v

export CMDBM_TEST_MARIA_HOST=127.0.0.1
export CMDBM_TEST_MARIA_PORT=13306
export CMDBM_TEST_MARIA_DB=cmdbmtest
export CMDBM_TEST_MARIA_USER=cmdbm
export CMDBM_TEST_MARIA_PASSWORD=cmdbm

export CMDBM_TEST_PGSQL_HOST=127.0.0.1
export CMDBM_TEST_PGSQL_PORT=15432
export CMDBM_TEST_PGSQL_DB=cmdbmtest
export CMDBM_TEST_PGSQL_USER=cmdbm
export CMDBM_TEST_PGSQL_PASSWORD=cmdbm
