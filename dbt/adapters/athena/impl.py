from datetime import date, datetime
from typing import Any, Dict, List, NamedTuple, Tuple
from urllib.parse import urlparse
from uuid import uuid4
import agate
import boto3

from dbt.adapters.base import available
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation


def _chunk_list(lst: List[Any], n: int) -> List[Any]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class SQLFilter(NamedTuple):
    column: str
    statement: str

    @classmethod
    def from_dict(cls, dict_filter) -> str:
        conditions = []
        for column, filters in dict_filter.items():
            conditions.append(cls.column_filter(column, filters))

        conditions = (condition.statement for condition in conditions)
        return " and ".join(conditions)

    @classmethod
    def column_filter(cls, column: str, filter: Dict):
        if not isinstance(filter, dict):
            raise ValueError(f"Filter must be a dict, got {type(filter)}.")

        methods = {
            "equals": cls.equals,
            "not_equals": cls.not_equals,
            "between": cls.between,
            "in": cls.in_,
        }
        for method_name, method in methods.items():
            if method_name in filter:
                return method(column, filter)
        raise ValueError(f"No valid filters found in {list(methods.keys())}")

    @classmethod
    def equals(cls, column: str, filter: Dict):
        value = filter["equals"]
        escaped = cls.escape(value)
        return SQLFilter(column, f"{column} = {escaped}")

    @classmethod
    def not_equals(cls, column: str, filter: Dict):
        value = filter["not_equals"]
        escaped = cls.escape(value)
        return SQLFilter(column, f"{column} <> {escaped}")

    @classmethod
    def between(cls, column: str, filter: Dict):
        start = filter["between"]["start"]
        end = filter["between"]["end"]
        escaped_start = cls.escape(start)
        escaped_end = cls.escape(end)
        return SQLFilter(column, f"{column} between {escaped_start} and {escaped_end}")

    @classmethod
    def in_(cls, column: str, filter: Dict):
        values = filter["in"]
        escaped = ", ".join((cls.escape(value) for value in values))
        return SQLFilter(column, f"{column} in ({escaped})")

    @classmethod
    def escape(cls, value: Any) -> str:
        if isinstance(value, dict) and "raw_sql" in value:
            return value["raw_sql"]
        return cls._terrible_basic_sql_escape(value)

    @staticmethod
    def _terrible_basic_sql_escape(value: Any) -> str:
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, (date, datetime)):
            return f"'{value}'"
        return str(value)


class AthenaAdapter(SQLAdapter):
    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "integer"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @available
    def s3_uuid_table_location(self):
        base_url = self._s3_base_location()
        return f"{base_url}/tables/{str(uuid4())}/"

    @available
    def s3_table_location(self, table_name: str) -> str:
        return f"{self._s3_base_location()}/{table_name}/"

    @available
    def s3_list_objects_in_partitions(
        self,
        relation: AthenaRelation,
        partitions: Dict[str, Any],
        allow_no_parttions: bool = False,
    ) -> List[str]:
        """Partitions is a dict of partition column name to value mapping.
        If value is a `dict` you can use an `IN` or `BETWEEN` condition.
        ```
        {
            "date": {
                "between": {
                    "start": "2021-05-01",
                    "end": "2021-05-31",
                }
            }
        }
        {
            "date": {
                "in": ["2021-05-01", "2021-05-02"]
            }
        }
        """
        query = f'select distinct "$path" as s3_url from {relation.render()}'
        conditions = SQLFilter.from_dict(partitions)
        where = " where " + conditions
        if conditions:
            query += where
        elif not allow_no_parttions:
            raise ValueError("Must provide `partitions`")

        print(query)
        _, table = self.execute(sql=query, auto_begin=True, fetch=True)
        s3_urls = [row["s3_url"] for row in table.rows]
        return s3_urls

    @available
    def s3_delete_objects(self, s3_urls: List[str]) -> None:
        if not s3_urls:
            return

        bucket_and_keys = [self._s3_bucket_and_object_key(url) for url in s3_urls]
        bucket = bucket_and_keys[0][0]

        s3 = boto3.client("s3")

        print(f"Deleting {len(bucket_and_keys)} objects from S3...")

        for chunk in _chunk_list(bucket_and_keys, 1000):
            s3.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": [{"Key": key} for _, key in chunk],
                    "Quiet": False,
                },
            )

    def _terrible_basic_sql_escape(self, value: Any) -> str:
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, (date, datetime)):
            return f"'{value}'"
        return str(value)

    def _s3_base_location(self):
        conn = self.connections.get_thread_connection()
        client = conn.handle

        url = client.s3_staging_dir.rstrip("/")
        return url

    def _s3_bucket_and_object_key(self, s3_url) -> Tuple[str, str]:
        parsed = urlparse(s3_url, allow_fragments=False)
        return parsed.netloc, parsed.path.lstrip("/")
