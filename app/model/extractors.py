from dataclasses import dataclass, field


@dataclass
class ListBreweriesConfigs:
    """
    Configuration class for managing brewery-related dataset queries.

    This dataclass holds configuration parameters for querying a dataset of breweries, such as pagination settings and options for extracting data. The settings defined here help in managing how the dataset is accessed and manipulated.

    Attributes:
        pages (list[int], optional):
            A list of integers specifying the page numbers to be retrieved from the dataset. Defaults to [1]. This attribute is ignored if `extract_all` is set to True.

        per_page (int, optional):
            The number of items to retrieve per page. Defaults to 20.

        extract_all (bool, optional):
            A flag indicating whether to extract all pages of the dataset. If True, the `pages` attribute is ignored, and all data from the dataset is extracted. Defaults to False.
    """

    pages: list[int] = field(default_factory=lambda: [1])
    per_page: int = field(default=100)
    extract_all: bool = field(default=False)
