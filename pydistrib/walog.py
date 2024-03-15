from time import clock_gettime_ns, CLOCK_REALTIME

import io
import ujson


class Entry[T]:
    index: int
    value: T
    timestamp: float

    def __init__(self, value: T, index: int) -> None:
        self.value = value
        self.index = index
        self.timestamp = clock_gettime_ns(CLOCK_REALTIME)

    def json(self) -> dict[str, str]:
        return {
            "index": str(self.index),
            "value": str(self.value),
            "timestamp": str(self.timestamp),
        }


type batch[T] = list[Entry[T]]


class WALog[T]:
    fp: io.TextIOWrapper
    index: int

    def __init__(self, file: str) -> None:
        self.fp = open(file=file, mode="a", encoding="UTF-8")
        self.index = 0

    def _convert(self, value: T) -> Entry[T]:
        self.index += 1
        return Entry(value=value, index=self.index)

    def write(self, value: T) -> None:
        return self._persist_batch(entries=[self._convert(value)])

    def write_many(self, values: list[T]) -> None:
        return self._persist_batch(entries=[self._convert(value) for value in values])

    def _persist_batch(self, entries: batch[T]) -> None:
        for entry in entries:
            ujson.dump(entry.json(), self.fp)
            self.fp.write("\n")
