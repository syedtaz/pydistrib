from pydistrib.heartbeat import *
import asyncio

recv = Receiver(host="127.0.0.1", port=8000, delay=3.0)
asyncio.run(recv.run())