from dataclasses import dataclass
from typing import Literal


@dataclass
class MedallionDataset:
    mount_location: str
    layer: Literal["bronze", "silver", "gold"]
