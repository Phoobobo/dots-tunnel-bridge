"""Optional client-side protocol connectors for an existing bridge server."""

from .openai import create_openai_connector

__all__ = ["create_openai_connector"]
