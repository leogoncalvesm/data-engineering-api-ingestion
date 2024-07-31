from typing import Any
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from app.model.breweries import BreweriesTable
from app.model.datasets import MedallionDataset
from app.domain.datasets import MedallionDatasetManager
from app.domain.breweries import LoadBreweriesTable


def main(**kwargs: dict[str, Any]):
    # Creating spark session
    builder = (
        SparkSession.builder.appName("Breweries Silver")
        .config("spark.master", "local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

    bronze_dataset_manager = MedallionDatasetManager(
        dataset=MedallionDataset(
            mount_location="./spark-warehouse/datasets/raw/", layer="bronze"
        )
    )
    silver_dataset_manager = MedallionDatasetManager(
        dataset=MedallionDataset(
            mount_location="./spark-warehouse/datasets/curated/", layer="silver"
        )
    )

    silver_loader = LoadBreweriesTable(
        spark=spark_session,
        bronze_dataset_manager=bronze_dataset_manager,
        silver_dataset_manager=silver_dataset_manager,
        metadata=BreweriesTable(),
    )
    silver_loader.run()

    spark_session.stop()


if __name__ == "__main__":
    main()
