from pydistrib.heartbeat import *
import asyncio

sender = Sender(id=1, host="127.0.0.1", port=8000, delay=10.0)
asyncio.run(sender.beat())