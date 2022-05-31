import abc
import dataclasses
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List

from papfa.dtos import Record


class ConsumerMiddleware(abc.ABC):
    def process_before_poll(self):
        pass

    def process_before_batching(self, message: Record) -> Record:
        return message

    def process_before_flush(self, batch: List[Record]) -> List[Record]:
        return batch

    def process_after_flush(self):
        pass


@dataclasses.dataclass(frozen=True, eq=True)
class MessageKey:
    topic: str
    group_id: str


class ConsumedMessageStatsMiddleware(ConsumerMiddleware):
    def __init__(self):
        self.consumed_message_stats = defaultdict(lambda: defaultdict(int))

    def process_before_flush(self, batch: List[Record]) -> List[Record]:
        for r in batch:
            key = MessageKey(topic=r.meta["topic"], group_id=r.meta["group_id"])
            self.consumed_message_stats[key]["last_message_timestamp"] = max(
                self.consumed_message_stats[key]["last_message_timestamp"], r.timestamp
            )
            self.consumed_message_stats[key]["consumed_message_timestamp"] = max(
                self.consumed_message_stats[key]["consumed_message_timestamp"],
                datetime.now().timestamp() * 10**3,  # to milliseconds
            )

        return batch

    def process_after_flush(self):
        for k, v in self.consumed_message_stats.items():
            Path("./consume-data/").mkdir(parents=True, exist_ok=True)
            _dir = f"{k.topic}-{k.group_id}.json"
            with open(f"./consume-data/{_dir}", "w") as f:
                json.dump(v, f)
