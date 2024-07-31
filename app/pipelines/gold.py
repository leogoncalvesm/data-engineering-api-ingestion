from typing import Any
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from app.model.breweries import BreweriesLocations
from app.model.datasets import MedallionDataset
from app.domain.datasets import MedallionDatasetManager
from app.domain.breweries import AggregatedBreweries


def main(**kwargs: dict[str, Any]):
    # Creating spark session
    builder = (
        SparkSession.builder.appName("Breweries Locations Gold")
        .config("spark.master", "local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

    gold_loader = AggregatedBreweries(
        spark=spark_session,
        metadata=BreweriesLocations(view_name="breweries_location"),
    )
    gold_loader.run()

    spark_session.stop()


if __name__ == "__main__":
    main()
