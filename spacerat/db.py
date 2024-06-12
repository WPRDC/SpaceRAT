import os
import re
from typing import Sequence, Mapping, Any

import psycopg2
import psycopg2.extras
from psycopg2.sql import Composable

from spacerat.const import DATASTORE_URL_ENV_VAR

connection_string = os.environ.get(DATASTORE_URL_ENV_VAR)


def query(
    q: str | bytes | Composable, vars: Sequence | Mapping[str, Any] | None = None
) -> list[dict]:
    with psycopg2.connect(connection_string) as conn:
        qry = str(re.sub(r"\s+", " ", q).strip())

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(qry, vars)
            results = cur.fetchall()
    conn.close()
    return results
