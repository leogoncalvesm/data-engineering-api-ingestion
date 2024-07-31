from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count_distinct, current_timestamp
from delta import DeltaTable

from app.domain.datasets import BaseDatasetManager
from app.gateways.breweries_api import BreweriesAPIGateway
from app.model.extractors import ListBreweriesConfigs
from app.model.breweries import BreweriesTable, BreweriesLocations


class AggregatedBreweries:
    """Create an aggregated view with the quantity of breweries per type and location."""

    def __init__(
        self,
        spark: SparkSession,
        metadata: BreweriesLocations,
    ) -> None:
        self.__spark = spark
        self.metadata = metadata

    def load_breweries_data(self) -> DataFrame:
        self.metadata.load_table(self.__spark)

    def process_table(self) -> DataFrame:
        return (
            self.metadata.breweries.groupBy(["country", "brewery_type"])
            .agg(count_distinct("id").alias("Total Breweries"))
            .withColumnsRenamed(self.metadata.renames)
            .orderBy(["Location", "Type", "Total Breweries"])
        )

    def create_view(self, df: DataFrame) -> None:
        df.createOrReplaceGlobalTempView(self.metadata.view_name)

    def show_results(self) -> None:
        self.__spark.sql(f"SELECT * FROM global_temp.{self.metadata.view_name}").show()

    def run(self) -> None:
        self.load_breweries_data()
        df = self.process_table()
        self.create_view(df)
        self.show_results()


class LoadBreweriesTable:
    def __init__(
        self,
        spark: SparkSession,
        bronze_dataset_manager: BaseDatasetManager,
        silver_dataset_manager: BaseDatasetManager,
        metadata: BreweriesTable,
    ) -> None:
        self.__spark = spark
        self.bronze_dataset = bronze_dataset_manager
        self.silver_dataset = silver_dataset_manager
        self.metadata = metadata

    def load_bronze_data(self) -> DataFrame:
        raw_files_path = (self.bronze_dataset.get_path() / "*.json").as_posix()
        return self.__spark.read.json(raw_files_path)

    def cast_geographical_columns(self, df: DataFrame) -> DataFrame:
        return df.withColumn("latitude", col("latitude").astype("float")).withColumn(
            "longitude", col("longitude").astype("float")
        )

    def include_ingest_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("ingest_timestamp", current_timestamp())

    def select_columns(self, df: DataFrame) -> DataFrame:
        return df.select(self.metadata.columns)

    def create_table(self, df: DataFrame, delta_table_path: str) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(*self.metadata.partition_columns)
            .save(delta_table_path)
        )

    def merge_changes(self, df: DataFrame, delta_table_path: str) -> None:
        delta_table = DeltaTable.forPath(self.__spark, delta_table_path)
        join_condition = " AND ".join(
            [f"cur.{k} = new.{k}" for k in self.metadata.primary_keys]
        )
        (
            delta_table.alias("cur")
            .merge(df.alias("new"), join_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def save_delta_table(self, df: DataFrame) -> None:
        delta_table_path = (
            self.silver_dataset.get_path() / self.metadata.table_name
        ).as_posix()

        if DeltaTable.isDeltaTable(self.__spark, delta_table_path):
            self.merge_changes(df, delta_table_path)
        else:
            self.create_table(df, delta_table_path)

    def move_bronze_data(self) -> None:
        self.bronze_dataset.create_directory("processed")
        self.bronze_dataset.move_files(
            source="", destination="processed", file_pattern="*.json"
        )

    def run(self) -> None:
        self.silver_dataset.setup_dataset()

        df = self.load_bronze_data()
        df = self.cast_geographical_columns(df=df)
        df = self.include_ingest_column(df=df)
        df = self.select_columns(df=df)

        self.save_delta_table(df=df)

        self.move_bronze_data()


class ExtractBreweriesAPI:
    def __init__(
        self,
        dataset_manager: BaseDatasetManager,
        configs: ListBreweriesConfigs,
        api_gateway: BreweriesAPIGateway,
    ) -> None:
        self.dataset_manager = dataset_manager
        self.configs = configs
        self.api_gateway = api_gateway

    def extract_page(self, page: int) -> bool:
        try:
            current_datetime = datetime.now().strftime("%Y-%m-%d")
            filename = f"{current_datetime}_raw_breweries_page_{page:09}.json"

            data = self.api_gateway.list_breweries(
                page=page, per_page=self.configs.per_page
            )
            self.dataset_manager.write_data(filename=filename, data=data)
        except:
            print(
                f"Error retrieving page {page}. Skipping..."
            )  # Should be a log function
            return False
        else:
            return True

    def extract_pages_list(self) -> None:
        for page in self.configs.pages:
            self.extract_page(page)

    def extract_all_pages(self) -> None:
        total_breweries = int(self.api_gateway.get_metadata().get("total"))
        total_pages = total_breweries // self.configs.per_page

        for page in range(1, total_pages + 1):
            self.extract_page(page)

    def run(self, **kwargs) -> None:
        self.dataset_manager.setup_dataset()

        if self.configs.extract_all:
            self.extract_all_pages(**kwargs)

        self.extract_pages_list(**kwargs)
