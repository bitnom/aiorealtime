import asyncio
import logging
from collections import defaultdict
from functools import wraps
from typing import Any, Callable, List, Dict, TypeVar, DefaultDict, Union

import aiohttp
from typing_extensions import ParamSpec

from realtime.channel import Channel
from realtime.exceptions import NotConnectedError
from realtime.message import HEARTBEAT_PAYLOAD, PHOENIX_CHANNEL, ChannelEvents, Message


T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")

logging.basicConfig(
    format="%(asctime)s:%(levelname)s - %(message)s", level=logging.INFO)


def ensure_connection(func: Callable[T_ParamSpec, T_Retval]):
    @wraps(func)
    async def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        if not args[0].connected:
            raise NotConnectedError(func.__name__)

        return await func(*args, **kwargs)

    return wrapper


class Socket:
    def __init__(self, url: str, auto_reconnect: bool = True, params=None, hb_interval: int = 10) -> None:
        """
        `Socket` is the abstraction for an actual socket connection that receives and 'reroutes' `Message` according to its `topic` and `event`.
        Socket-Channel has a 1-many relationship.
        Socket-Topic has a 1-many relationship.
        :param url: Websocket URL of the Realtime server. starts with `ws://` or `wss://`
        :param params: Optional parameters for connection.
        :param hb_interval: WS connection is kept alive by sending a heartbeat message. Optional, defaults to 30.
        """
        if params is None:
            params = {}
        self.url = url
        self.channels: DefaultDict[str, List[Channel]] = defaultdict(list)
        self.connected = False
        self.params = params
        self.hb_interval = hb_interval
        self.ws_connection: aiohttp.ClientWebSocketResponse = Union[Any, None]
        self.auto_reconnect = auto_reconnect
        self.logger = logging.getLogger("Socket")
        self.session: aiohttp.ClientSession = Union[Any, None]
        self.listen_task = None
        self.keep_alive_task = None

    async def connect(self) -> None:
        """
        Establishes the websocket connection.
        """
        self.session = aiohttp.ClientSession()
        try:
            self.ws_connection = await self.session.ws_connect(self.url)
            self.connected = True
            self.listen_task = asyncio.create_task(self._listen())
            self.keep_alive_task = asyncio.create_task(self._keep_alive())
        except Exception as e:
            self.logger.error(f"Error connecting to WebSocket: {e}")
            await self.session.close()
            raise

    async def shutdown(self) -> None:
        # Cancel the listen and keep-alive tasks
        if self.listen_task:
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                self.logger.info("Listen task cancelled.")
        if self.keep_alive_task:
            self.keep_alive_task.cancel()
            try:
                await self.keep_alive_task
            except asyncio.CancelledError:
                self.logger.info("Keep-alive task cancelled.")

        # Close the WebSocket connection
        if self.ws_connection:
            await self.ws_connection.close()
            self.logger.info("WebSocket connection closed.")

        # Close the aiohttp session
        if self.session:
            await self.session.close()
            self.logger.info("HTTP session closed.")

        self.connected = False

    @ensure_connection
    async def _listen(self) -> None:
        """
        An infinite loop that keeps listening.
        :return: None
        """
        async def listen_to_messages():
            while True:
                if self.ws_connection is None:
                    await asyncio.sleep(1)  # Wait a bit before trying to reconnect or handle the lack of connection
                    continue

                try:
                    msg = await self.ws_connection.receive_json()
                    msg = Message(**msg)

                    if msg.event == ChannelEvents.reply:
                        continue

                    for channel in self.channels.get(msg.topic, []):
                        if channel.joined:
                            for cl in channel.listeners:
                                if cl.event in ["*", msg.event]:
                                    await cl.callback(msg.payload)
                except Exception as e:
                    self.logger.error(f"Error receiving message: {e}")
                    if self.auto_reconnect:
                        self.logger.info("Connection with server closed, trying to reconnect...")
                        await asyncio.sleep(2)
                        await self.connect()
                    else:
                        self.logger.exception("Connection with the server closed.")
                        break

        await listen_to_messages()

    async def _keep_alive(self) -> None:
        """
        Sending heartbeat to server every 5 seconds
        Ping - pong messages to verify connection is alive
        """
        while True:
            try:
                if self.ws_connection:
                    await self.ws_connection.send_json({
                        "topic": PHOENIX_CHANNEL,
                        "event": ChannelEvents.heartbeat,
                        "payload": HEARTBEAT_PAYLOAD,
                        "ref": None,
                    })
                await asyncio.sleep(self.hb_interval)
            except Exception as e:
                self.logger.error(f"Error sending heartbeat: {e}")
                if self.auto_reconnect:
                    self.logger.info("Connection with server closed, trying to reconnect...")
                    await self.connect()
                else:
                    self.logger.exception("Connection with the server closed.")
                    break

    @ensure_connection
    async def set_channel(self, topic: str) -> Channel:
        """
        :param topic: Initializes a channel and creates a two-way association with the socket
        :return: Channel
        """
        chan = Channel(self, topic, self.params)
        self.channels[topic].append(chan)
        await chan.join()
        return chan

    async def close(self) -> None:
        """
        Closes the WebSocket connection and the aiohttp session.
        """
        if self.ws_connection:
            await self.ws_connection.close()
        if self.session:
            await self.session.close()
        self.connected = False

    def summary(self) -> None:
        """
        Prints a list of topics and event the socket is listening to
        :return: None
        """
        for topic, chans in self.channels.items():
            for chan in chans:
                print(
                    f"Topic: {topic} | Events: {[e.event for e in chan.listeners]}")
