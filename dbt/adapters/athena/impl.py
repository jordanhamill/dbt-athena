from datetime import date, datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
from uuid import uuid4
import agate
import boto3

from dbt.adapters.base import available
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.athena import AthenaConnectionManager
from dbt.adapters.athena.relation import AthenaRelation


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
        query = f'select "$path" as s3_url from {relation.render()}'
        conditions = []
        for partition_column, partition_value in partitions.items():
            if isinstance(partition_value, dict):
                if "between" in partition_value:
                    between = partition_value["between"]
                    start = self._terrible_basic_sql_escape(between["start"])
                    end = self._terrible_basic_sql_escape(between["end"])
                    conditions.append(f"{partition_column} between {start} and {end}")
                elif "in" in partition_value:
                    comma_separated = ", ".join(
                        (
                            self._terrible_basic_sql_escape(value)
                            for value in partition_value["in"]
                        )
                    )
                conditions.append(f"{partition_column} in ({comma_separated})")
            else:
                conditions.append(
                    f"{partition_column} = {self._terrible_basic_sql_escape(partition_value)}"
                )

        where = " where " + (" and ".join(conditions))
        if conditions:
            query += where
        elif not allow_no_parttions:
            raise ValueError("Must provide `partitions`")

        _, table = self.execute(sql=query, auto_begin=True, fetch=True)
        s3_urls = [row["s3_url"] for row in table.rows]
        return s3_urls

    @available
    def s3_delete_objects(self, s3_urls: List[str]) -> None:
        if not s3_urls:
            return

        if len(s3_urls) > 1000:
            assert False, "Need to allow deleting more than 1000 parquet files"

        bucket_and_keys = [self._s3_bucket_and_object_key(url) for url in s3_urls]
        bucket = bucket_and_keys[0][0]

        s3 = boto3.client("s3")
        s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": key} for _, key in bucket_and_keys],
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

    def _s3_delete_objects(self, bucket: str, key_prefix: str) -> None:
        if not key_prefix:
            return

        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=key_prefix).delete()
