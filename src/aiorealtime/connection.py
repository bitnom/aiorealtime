import asyncio
import logging
import os
from collections import defaultdict
from functools import wraps
from typing import Any, Callable, List, Dict, TypeVar, DefaultDict, Union, Optional

import aiohttp
from typing_extensions import ParamSpec

from aiorealtime.channel import Channel
from aiorealtime.exceptions import NotConnectedError
from aiorealtime.message import HEARTBEAT_PAYLOAD, PHOENIX_CHANNEL, ChannelEvents, Message
from aiorealtime.utils import create_realtime_url, table_path_to_realtime

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class Socket:
    def __init__(self, url: str, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.url = url
        self.loop = loop or asyncio.get_event_loop()
        self.ws_connection: aiohttp.ClientWebSocketResponse
        self.logger = logging.getLogger(__name__)
        self.channels: DefaultDict[str, List[Channel]] = defaultdict(list)
        self.session: aiohttp.ClientSession
        self.keep_alive_task = None

    async def connect(self):
        """
        Connect to the WebSocket server.
        """
        self.session = aiohttp.ClientSession()  # Store the session instance
        try:
            self.ws_connection = await self.session.ws_connect(self.url)
            self.logger.info(f"Connected to WebSocket at {self.url}")
            if self.keep_alive_task is None or self.keep_alive_task.done():
                self.keep_alive_task = asyncio.create_task(self._keep_alive())
        except Exception as e:
            self.logger.error(f"Failed to connect to WebSocket at {self.url}: {e}")
            await self.session.close()  # Close the session if connection fails
            raise

    async def _keep_alive(self, interval: float = 5.0):
        """
        Send a heartbeat message periodically to keep the WebSocket connection alive.

        :param interval: The interval in seconds between each heartbeat message.
        """
        while True:
            try:
                # Check if the WebSocket connection is still open
                if self.ws_connection is None or self.ws_connection.closed:
                    self.logger.info("_keep_alive detected closed WebSocket connection.")
                    raise NotConnectedError

                # Send the heartbeat message
                await self.ws_connection.send_json({
                    "topic": PHOENIX_CHANNEL,
                    "event": ChannelEvents.heartbeat,
                    "payload": HEARTBEAT_PAYLOAD,
                    "ref": None,
                })
                self.logger.debug("Heartbeat message sent to keep the WebSocket connection alive.")

                # Wait for the specified interval before sending the next heartbeat
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                # The keep-alive task was cancelled, likely due to a disconnect
                self.logger.info("Keep-alive task cancelled.")
                break
            except NotConnectedError:

            except Exception as e:
                self.logger.error(f"An error occurred while sending a heartbeat message: {e}")
                break

    async def disconnect(self):
        """
        Disconnect from the WebSocket server.
        """
        if self.ws_connection:
            await self.ws_connection.close()
            self.ws_connection = None
            self.logger.info(f"Disconnected from WebSocket at {self.url}")
        if hasattr(self, 'session') and self.session:  # Check if the session exists and close it
            await self.session.close()
            self.session = None

    async def send_json(self, data):
        """
        Send JSON data through the WebSocket connection.
        """
        if not self.ws_connection:
            raise RuntimeError("WebSocket connection is not established.")
        await self.ws_connection.send_json(data)

    async def receive_json(self):
        """
        Receive JSON data from the WebSocket connection.
        """
        if not self.ws_connection:
            raise RuntimeError("WebSocket connection is not established.")
        return await self.ws_connection.receive_json()

    async def set_channel(self, topic: str, params=None, reconnect=False):
        """
        Create a Channel instance associated with this Socket.
        """
        if reconnect:
            for topic, channels in self.channels.items():
                for chan in channels:
                    await chan.join()
            return
        chan = Channel(self, topic=topic, params=params)
        self.channels[topic].append(chan)
        await chan.join()
        return chan

    async def listen(self):
        """
        Continuously listen for incoming messages from the WebSocket connection.
        """
        if not self.ws_connection:
            raise RuntimeError("WebSocket connection is not established.")
        try:
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self.logger.info(f"Received message: {msg}")
                    # Process the message here (e.g., dispatch to channel listeners)
                    data = msg.json()
                    if not isinstance(data, dict) or 'event' not in data:
                        self.logger.warning(f"Received invalid message format: {data}")
                        continue

                    message = Message(**data)

                    if message.event == ChannelEvents.reply:
                        continue

                    for channel in self.channels.get(message.topic, []):
                        if channel.joined:
                            for cl in channel.listeners:
                                if cl.event in ["*", message.event]:
                                    await cl.callback(message.payload)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"WebSocket connection closed with exception {self.ws_connection.exception()}")
                    break
        except asyncio.CancelledError:
            # The listen loop was cancelled, likely due to a disconnect
            self.logger.info("WebSocket listen loop cancelled.")
        finally:
            await self.disconnect()


async def callback1(payload):
    print("Callback: ", payload)


if __name__ == "__main__":
    # Example usage
    async def main():
        api_url = "https://mmtkmitdhqjmfmmllvez.supabase.co"
        api_key = os.getenv('SUPABASE_API_KEY')
        s = Socket(create_realtime_url(api_url, api_key))
        try:
            await s.connect()
            channel = await s.set_channel(table_path_to_realtime('public.chat_messages'))
            channel.on('*', callback1)
            # Start listening for messages
            await s.listen()  # This will block until the connection is closed
        finally:
            # Ensure the disconnect method is called to close the session properly
            await s.disconnect()


    asyncio.run(main())
