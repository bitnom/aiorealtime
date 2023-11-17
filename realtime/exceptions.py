class NotConnectedError(Exception):
    """
    Raised when operations requiring a connection are executed when socket is not connected
    """

    def __init__(self, func_name: str):
        self.offending_func_name: str = func_name

    def __str__(self):
        return f"A WS connection has not been established. Ensure you call Socket.connect() before calling Socket.{self.offending_func_name}()"


class ChannelJoinError(Exception):
    """Raised when a channel fails to join."""

    def __init__(self, topic: str, message: str):
        self.topic = topic
        self.message = message

    def __str__(self):
        return f"Failed to join the topic '{self.topic}': {self.message}"
