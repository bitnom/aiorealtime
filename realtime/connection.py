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
    format="%(asctime)s:%(levelname)s - %(message)s", level=logging.WARN)


def ensure_connection(func: Callable[T_ParamSpec, T_Retval]):
    @wraps(func)
    async def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        self = args[0]
        if not self.connected:
            self.logger.warning(f"Attempted to call '{func.__name__}' without an active connection.")
            return  # Optionally, you could raise an exception or handle reconnection here
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
        self.logger.setLevel(logging.WARN)
        self.session: aiohttp.ClientSession = Union[Any, None]
        self.listen_task = None
        self.keep_alive_task = None
        self.reconnect_lock = asyncio.Lock()
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.timeout_float = 10.0
        self.shutdown_lock = asyncio.Lock()
        self.receive_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self.reconnect_lock:
            if self.connected:
                return  # Already connected, no need to connect again

            # Close any existing session and connection before reconnecting
            await self.shutdown()

            self.session = aiohttp.ClientSession(timeout=self.timeout)
            try:
                self.ws_connection = await self.session.ws_connect(
                    self.url,
                    timeout=self.timeout_float,
                    # receive_timeout=self.timeout_float
                )
                self.connected = True
                self.logger.info('Realtime reconnected')
                if self.listen_task is None or self.listen_task.done():
                    self.listen_task = asyncio.create_task(self._listen())
                if self.keep_alive_task is None or self.keep_alive_task.done():
                    self.keep_alive_task = asyncio.create_task(self._keep_alive())
            except Exception as e:
                self.logger.error(f"Error connecting to WebSocket: {e}")
                await self.shutdown()
                raise

    async def shutdown(self) -> None:
        async with self.shutdown_lock:
            if self.listen_task and not self.listen_task.done():
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    self.logger.info("Listen task cancelled.")
            self.listen_task = None

            if self.keep_alive_task and not self.keep_alive_task.done():
                self.keep_alive_task.cancel()
                try:
                    await self.keep_alive_task
                except asyncio.CancelledError:
                    self.logger.info("Keep-alive task cancelled.")
            self.keep_alive_task = None

        # Close the WebSocket connection
        if hasattr(self.ws_connection, 'close'):
            await self.ws_connection.close()
            self.logger.info("WebSocket connection closed.")

        # Close the aiohttp session
        if hasattr(self.session, 'close'):
            await self.session.close()
            self.logger.info("HTTP session closed.")

        self.connected = False

    @ensure_connection
    async def _listen(self) -> None:
        async def listen_to_messages():
            while self.connected:
                if self.ws_connection is None:
                    await asyncio.sleep(1)  # Wait a bit before trying to reconnect or handle the lack of connection
                    continue

                try:
                    async with self.receive_lock:
                        msg = await self.ws_connection.receive()
                        self.logger.debug(f"Realtime - received: {msg}")
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if not isinstance(data, dict) or 'event' not in data:
                            self.logger.error(f"Received invalid message format: {data}")
                            continue

                        message = Message(**data)

                        if message.event == ChannelEvents.reply:
                            continue

                        for channel in self.channels.get(message.topic, []):
                            if channel.joined:
                                for cl in channel.listeners:
                                    if cl.event in ["*", message.event]:
                                        await cl.callback(message.payload)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                except TimeoutError as e:
                    self.logger.error(f"Realtime - Timeout error receiving message: {e}")
                    await self.close()
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
            async with self.shutdown_lock:
                self.logger.debug('Realtime sending heartbeat...')
                try:
                    if self.ws_connection:
                        await self.ws_connection.send_json({
                            "topic": PHOENIX_CHANNEL,
                            "event": ChannelEvents.heartbeat,
                            "payload": HEARTBEAT_PAYLOAD,
                            "ref": None,
                        })
                    self.logger.debug('Realtime sent heartbeat')
                    await asyncio.sleep(self.hb_interval)
                except Exception as e:
                    self.logger.error(f"Error sending heartbeat: {e}")
                    await self.close()
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
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
            self.ws_connection = None
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
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
