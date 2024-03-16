from time import clock_gettime_ns, CLOCK_REALTIME
from dataclasses import dataclass
from typing import Callable, Coroutine

import asyncio


@dataclass
class Sender:
    id: int
    host: str
    port: int
    delay: float

    async def beat(self) -> None:

        while True:
            _, writer = await asyncio.open_connection(host=self.host, port=self.port)
            writer.write(str.encode(f"{self.id},{clock_gettime_ns(CLOCK_REALTIME)}"))
            writer.close()
            await asyncio.sleep(self.delay)


class Receiver:
    senders: dict[int, int]
    delay: float
    reader: asyncio.StreamReader
    serve_func: Callable[
        [asyncio.StreamReader, asyncio.StreamWriter], Coroutine[None, None, None]
    ]
    host: str
    port: int

    def __init__(self, host: str, port: int, delay: float) -> None:

        self.senders, self.delay, self.host, self.port = {}, delay, host, port

        async def receive_heartbeat(
            reader: asyncio.StreamReader, _writer: asyncio.StreamWriter
        ) -> None:
            data = await reader.read()
            id, time = data.decode("utf-8").strip().split(",")
            self.senders[int(id)] = int(time)
            print(f">> Updated {id}")

        self.serve_func = receive_heartbeat

    async def run(self) -> None:
        server = await asyncio.start_server(self.serve_func, self.host, self.port)
        await self.check()
        await server.serve_forever()


    async def check(self) -> None:

        while True:
            current = clock_gettime_ns(CLOCK_REALTIME)
            for k, v in self.senders.items():
                if current - v > self.delay:
                    print(f">> {clock_gettime_ns(CLOCK_REALTIME)} : {k} is down")

            await asyncio.sleep(self.delay)
