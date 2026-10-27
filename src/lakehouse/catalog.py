import os

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType, TimestamptzType

from src.database.connection import get_database_url

NAMESPACE = "bronze"
PRICE_EVENTS = f"{NAMESPACE}.price_events"

PRICE_EVENTS_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "schema_version", IntegerType()),
    NestedField(3, "exchange", StringType()),
    NestedField(4, "pair", StringType()),
    NestedField(5, "base_asset", StringType()),
    NestedField(6, "price", DoubleType()),
    NestedField(7, "open_24h", DoubleType()),
    NestedField(8, "high_24h", DoubleType()),
    NestedField(9, "low_24h", DoubleType()),
    NestedField(10, "volume_24h_quote", DoubleType()),
    NestedField(11, "event_time", TimestamptzType(), required=True),
    NestedField(12, "ingested_at", TimestamptzType()),
    # Added later by schema evolution (Step 47.7); listed here so a new environment
    # creates the same table
    NestedField(13, "quote_asset", StringType(), doc="Quote currency, e.g. USDT"),
    identifier_field_ids=[1],
)

# One folder of files per UTC day; queries filtered on event_time skip other days.
PRICE_EVENTS_PARTITIONING = PartitionSpec(
    PartitionField(source_id=11, field_id=1000, transform=DayTransform(), name="event_day")
)

# Inside each file, rows are sorted by coin, so a filter on base_asset reads less.
PRICE_EVENTS_SORT = SortOrder(SortField(source_id=5, transform=IdentityTransform()))


def get_catalog():
    """Iceberg catalog: table metadata in PostgreSQL, data files in object storage."""
    properties = {
        # The same POSTGRESQL_* settings get_engine() uses
        "uri": get_database_url().render_as_string(hide_password=False),
        "warehouse": f"s3://{os.environ.get('LAKE_BUCKET', 'riskcoin-lake')}/warehouse",
        "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
    }
    # Local S3-compatible storage needs an endpoint and keys. On AWS (Step 51) leave
    # these unset and the EC2 instance role provides credentials.
    if os.environ.get("S3_ENDPOINT"):
        properties["s3.endpoint"] = os.environ["S3_ENDPOINT"]
        properties["s3.access-key-id"] = os.environ["S3_ACCESS_KEY"]
        properties["s3.secret-access-key"] = os.environ["S3_SECRET_KEY"]
    return SqlCatalog("riskcoin", **properties)


def get_price_events_table(catalog=None):
    catalog = catalog or get_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog.create_table_if_not_exists(
        PRICE_EVENTS,
        schema=PRICE_EVENTS_SCHEMA,
        partition_spec=PRICE_EVENTS_PARTITIONING,
        sort_order=PRICE_EVENTS_SORT,
    )
