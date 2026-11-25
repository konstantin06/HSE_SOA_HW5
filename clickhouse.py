import json
import os

import httpx

URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
AUTH = (os.getenv("CLICKHOUSE_USER", "cinema"), os.getenv("CLICKHOUSE_PASSWORD", "cinema_demo"))


def query(sql, day=None, data=None, parameters=None):
    params = {"wait_end_of_query": "1", "output_format_json_quote_64bit_integers": "0"}
    for name, item in (parameters or {}).items():
        params[f"param_{name}"] = str(item)
    if day is not None:
        params["param_day"] = day.isoformat()
    if data is None:
        body = sql + "\nFORMAT JSONEachRow"
    else:
        body = sql + "\n" + json.dumps(data, allow_nan=False)
    response = httpx.post(URL, params=params, content=body.encode(), auth=AUTH, timeout=30)
    if response.is_error:
        raise RuntimeError(f"ClickHouse {response.status_code}: {response.text[:1000]}")
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]
