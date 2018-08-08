#!/usr/bin/env python

import os, resource
import asyncio


class PortScanner:
    def __init__(self, host="0.0.0.0", ports=range(1, 1024 + 1), batch_size=1024):
        self.host = host
        self.ports = ports
        self.semaphore = asyncio.Semaphore(value=batch_size)
        self.loop = asyncio.get_event_loop()

    async def scan_port(self, port, timeout):
        async with self.semaphore:
            try:
                future = asyncio.open_connection(self.host, port, loop=self.loop)
                reader, writer = await asyncio.wait_for(future, timeout=timeout)
                print("{} open".format(port))
                writer.close()
            except ConnectionRefusedError:
                pass
                # print("{} closed".format(port))
            except asyncio.TimeoutError:
                pass
                # print("{} timeout".format(port))
            except Exception as ex:
                print(ex)

    def start(self, timeout=1.0):
        self.loop.run_until_complete(asyncio.gather(
            *[self.scan_port(port, timeout) for port in self.ports]
        ))


limits = resource.getrlimit(resource.RLIMIT_NOFILE)
batch_size = min(512, limits[0])

scanner = PortScanner(host="127.0.0.1", ports=range(1, 65535 + 1), batch_size=batch_size)

scanner.start()
