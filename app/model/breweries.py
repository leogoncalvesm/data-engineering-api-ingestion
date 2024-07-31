from dataclasses import dataclass, field
from pyspark.sql import DataFrame, SparkSession


@dataclass
class BreweriesTable:
    primary_keys: list[str] = field(default_factory=lambda: ["id"])
    columns: list[str] = field(
        default_factory=lambda: [
            "id",
            "brewery_type",
            "name",
            "country",
            "state",
            "state_province",
            "city",
            "latitude",
            "longitude",
            "postal_code",
            "street",
            "address_1",
            "address_2",
            "address_3",
            "phone",
            "website_url",
            "ingest_timestamp",
        ]
    )
    partition_columns: list[str] = field(
        default_factory=lambda: [
            "country",
            "state",
            "city",
        ]
    )
    table_name: str = field(init=False, default="breweries")


@dataclass
class BreweriesLocations:
    view_name: str
    breweries: DataFrame = field(init=False)

    # In case we're writting to some catalog
    column_descriptions: list[str] = field(
        default_factory=lambda: {
            "Location": "Country of the brewery",
            "Type": "Type/size of the brewery",
            "Total Breweries": "Total amount of breweries with that `Type` within the `Location`",
        }
    )
    renames: dict[str, str] = field(
        default_factory=lambda: {"country": "Location", "brewery_type": "Type"}
    )

    def load_table(self, spark: SparkSession) -> None:
        self.breweries = spark.read.format("delta").load(
            "spark-warehouse/datasets/curated/breweries/"
        )
