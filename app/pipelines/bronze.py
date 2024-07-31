from typing import Any

from app.model.datasets import MedallionDataset
from app.model.extractors import ListBreweriesConfigs
from app.domain.breweries import ExtractBreweriesAPI
from app.domain.datasets import MedallionDatasetManager
from app.gateways.breweries_api import BreweriesAPIGateway


def main(**kwargs: dict[str, Any]):
    breweries_configs = {
        k: v for k, v in kwargs.items() if k in {"pages", "per_page", "extract_all"}
    }
    configs = ListBreweriesConfigs(**breweries_configs)
    dataset_manager = MedallionDatasetManager(
        dataset=MedallionDataset(
            mount_location="./spark-warehouse/datasets/raw/", layer="bronze"
        )
    )
    api_gateway = BreweriesAPIGateway()

    bronze_extractor = ExtractBreweriesAPI(
        dataset_manager=dataset_manager, configs=configs, api_gateway=api_gateway
    )

    bronze_extractor.run()


if __name__ == "__main__":
    main()
