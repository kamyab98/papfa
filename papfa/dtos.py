import dataclasses
from typing import Union, Optional


@dataclasses.dataclass(frozen=True)
class Record:
    value: dict
    timestamp: Optional[int]  # The number of *milliseconds* since the epoch
    key: Union[str, dict, None] = None
    headers: dict = dataclasses.field(default_factory=dict)
    meta: dict = dataclasses.field(default_factory=dict)
