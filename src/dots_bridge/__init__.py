"""HTTP and MCP Events integration for OpenAI dots."""

from .app import create_app
from .bridge import DotsBridge
from .client import AsyncBridgeClient, BridgeClient, ReplyTimeoutError
from .config import Settings
from .errors import BridgeError
from .models import ReceivedMessage

__version__ = "0.2.0"
__all__ = [
    "AsyncBridgeClient",
    "BridgeClient",
    "BridgeError",
    "DotsBridge",
    "ReplyTimeoutError",
    "ReceivedMessage",
    "Settings",
    "create_app",
]
