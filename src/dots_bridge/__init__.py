"""HTTP and MCP Events integration for OpenAI dots."""

from .app import create_app
from .bridge import DotsBridge
from .client import AsyncBridgeClient, BridgeClient, ReplyTimeoutError
from .config import Settings
from .errors import BridgeError

__version__ = "0.1.0"
__all__ = [
    "AsyncBridgeClient",
    "BridgeClient",
    "BridgeError",
    "DotsBridge",
    "ReplyTimeoutError",
    "Settings",
    "create_app",
]
