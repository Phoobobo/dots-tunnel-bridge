class BridgeError(Exception):
    """An error safe to return through HTTP and MCP (contains no credentials)."""

    def __init__(self, code: str, message: str, status_code: int = 400, rpc_code: int = -32602):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.rpc_code = rpc_code
