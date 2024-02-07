from __future__ import annotations

from typing import List, TYPE_CHECKING, NamedTuple

from aiorealtime.types import Callback

if TYPE_CHECKING:
    from aiorealtime.connection import Socket


class CallbackListener(NamedTuple):
    """A tuple with `event` and `callback` """
    event: str
    callback: Callback


class Channel:
    """
    `Channel` is an abstraction for a topic listener for an existing socket connection.
    Each Channel has its own topic and a list of event-callbacks that responds to messages.
    Should only be instantiated through `connection.Socket().set_channel(topic)`
    Topic-Channel has a 1-many relationship.
    """

    def __init__(self, socket: Socket, topic: str, params=None) -> None:
        """
        :param socket: Socket object
        :param topic: Topic that it subscribes to on the realtime server
        :param params:
        """
        if params is None:
            params = {}
        self.socket = socket
        self.params = params
        self.topic = topic
        self.listeners: List[CallbackListener] = []
        self.joined = False

    async def join(self) -> 'Channel':
        """
        Coroutine that attempts to join Phoenix Realtime server via a certain topic
        :return: None
        """
        join_req = {"topic": self.topic, "event": "phx_join", "payload": {}, "ref": None}
        try:
            await self.socket.ws_connection.send_json(join_req)
            self.joined = True
            return self  # Return the Channel instance to allow chaining
        except Exception as e:
            self.socket.logger.error(f"Error joining channel '{self.topic}': {e}")
            raise

    def on(self, event: str, callback: Callback) -> Channel:
        """
        :param event: A specific event will have a specific callback
        :param callback: Callback that takes msg payload as its first argument
        :return: Channel
        """
        cl = CallbackListener(event=event, callback=callback)
        self.listeners.append(cl)
        return self

    def off(self, event: str) -> None:
        """
        :param event: Stop responding to a certain event
        :return: None
        """
        self.listeners = [callback for callback in self.listeners if callback.event != event]
