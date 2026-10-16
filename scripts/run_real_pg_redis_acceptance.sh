#!/usr/bin/env bash
set -Eeuo pipefail

# Disposable real-backend acceptance run. Everything lives under /tmp and is
# removed by the test fixture when the run exits.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="${AGENTSCOPE_TEST_PYTHON:-/tmp/asvenv}"
mkdir -p "${VENV}"
if [[ ! -x "${VENV}/bin/python" ]]; then
  python3 -m venv "${VENV}"
fi
"${VENV}/bin/python" -m pip install -q --disable-pip-version-check \
  pytest asyncpg redis sqlalchemy alembic aiosqlite fakeredis
"${VENV}/bin/python" -m pip install -q --disable-pip-version-check \
  pgserver redislite

PG_BIN="${AGENTSCOPE_PG_BIN:-$(${VENV}/bin/python - <<'PY'
from pathlib import Path
import pgserver
print(Path(pgserver.__file__).parent / "pginstall" / "bin")
PY
)}"
REDIS_SERVER="${AGENTSCOPE_REDIS_SERVER:-${VENV}/bin/redis-server}"
[[ -x "${PG_BIN}/initdb" && -x "${PG_BIN}/postgres" ]] || {
  echo "PostgreSQL binaries unavailable: ${PG_BIN}" >&2; exit 2;
}
[[ -x "${REDIS_SERVER}" ]] || {
  echo "Redis server unavailable: ${REDIS_SERVER}" >&2; exit 2;
}

cd "${ROOT_DIR}"
AGENTSCOPE_REAL_BACKENDS=1 \
AGENTSCOPE_PG_BIN="${PG_BIN}" \
AGENTSCOPE_REDIS_SERVER="${REDIS_SERVER}" \
PYTHONPATH="${ROOT_DIR}/src" \
"${VENV}/bin/python" -m pytest -q \
  tests/real_pg_redis_service_acceptance_test.py --disable-warnings --maxfail=1
