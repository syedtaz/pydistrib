from time import clock_gettime_ns, CLOCK_REALTIME

import io
import ujson
import os


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
    base: int
    base_low: int
    max_size: int
    filename: str

    def __init__(self, filename: str, max_size: int = 1024) -> None:
        self.filename = filename
        self.base = 0
        self.base_low = 0
        self.fp = open(file=f"{self.filename}_{self.base}", mode="a", encoding="UTF-8")
        self.index = 0
        self.max_size = max_size

    def _maybe_roll(self) -> None:
        if (new_offset := self.index // self.max_size) <= self.base:
            return

        self.base = new_offset
        self.fp.flush()
        self.fp.close()
        self.fp = open(file=f"{self.filename}_{self.base}", mode="a", encoding="UTF-8")

    def _convert(self, value: T) -> Entry[T]:
        self._maybe_roll()
        self.index += 1
        return Entry(value=value, index=self.index)

    def _maybe_discard_logs(self) -> None:
        if abs(self.base - self.base_low) >= 5:
            self.base_low = self.base
            self._discard_logs()

    def _discard_logs(self) -> None:

        candidates = [file for file in os.listdir() if self.filename in file]
        candidates = [file for file in candidates if int(file.split("_")[-1]) < self.base_low]

        for file in candidates:
            os.remove(file)

    def write(self, value: T) -> None:
        return self._persist_batch(entries=[self._convert(value)])

    def write_many(self, values: list[T]) -> None:
        return self._persist_batch(entries=[self._convert(value) for value in values])

    def _persist_batch(self, entries: list[Entry[T]]) -> None:
        for entry in entries:
            ujson.dump(entry.json(), self.fp)
            self.fp.write("\n")

        self.fp.flush()
        self._maybe_discard_logs()