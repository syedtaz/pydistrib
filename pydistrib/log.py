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


class Log[T]:
    fp: io.TextIOWrapper
    index: int
    offset: int
    max_size: int
    filename: str

    def __init__(self, filename: str, max_size: int = 1024) -> None:
        self.filename = filename
        self.offset = 0
        self.fp = open(file=f"{self.filename}_{self.offset}", mode="a", encoding="UTF-8")
        self.index = 0
        self.max_size = max_size


    def _maybe_roll(self) -> None:
        if (new_offset := self.index // self.max_size) <= self.offset:
            return

        self.offset = new_offset
        self.fp.flush()
        self.fp.close()
        self.fp = open(file=f"{self.filename}_{self.offset}", mode="a", encoding="UTF-8")

    def _convert(self, value: T) -> Entry[T]:
        self._maybe_roll()
        self.index += 1
        return Entry(value=value, index=self.index)

    def write(self, value: T) -> None:
        return self._persist_batch(entries=[self._convert(value)])

    def write_many(self, values: list[T]) -> None:
        return self._persist_batch(entries=[self._convert(value) for value in values])

    def _persist_batch(self, entries: list[Entry[T]]) -> None:
        for entry in entries:
            ujson.dump(entry.json(), self.fp)
            self.fp.write("\n")
