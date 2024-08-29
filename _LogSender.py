class _LogSender(aio_components.BaseServiceAIOComponent,
                 base_logger.handler(separator=None,  # type: ignore[misc]
                                     lock_creator=lambda _: lib_concurrent.RLock())):
    name = "LogSender"

    _record_properties = frozenset((
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename", "module",
        "lineno", "funcName", "created", "msecs", "relativeCreated", "thread",
        "threadName", "processName", "process", "stack_info", "exc_info", "exc_text", "chain"
    ))

    class TransportScheme(enum.Enum):
        UDP = "udp"
        TCP = "tcp"

    def __init__(self, *args, **kw) -> None:
        super().__init__(*args, **kw)
        self._log_url: URL = URL(self.service.log_url)
        self._transport_scheme: _LogSender.TransportScheme = _LogSender.TransportScheme(self._log_url.scheme)
        self._handler: Optional[Callable[[bytes], None]] = None
        self._pending_log_records: List[bytes] = []
        self._transport: Optional[Union[asyncio.DatagramTransport, socket.SocketType]] = None

    def emit(self, record: logging.LogRecord) -> None:
        if self._handler is not None:
            self._handler(self._process_record(record))
        else:
            self._pending_log_records.append(self._process_record(record))

    def _process_record(self, record: logging.LogRecord) -> bytes:
        record_data = {
            key: value for key, value in record.__dict__.items() if key in self._record_properties
        }
        record_data["service_uuid"] = self.service.uuid
        record_data["service_name"] = self.service.name
        record_data["service_type"] = self.service.type
        record_data["node_id"] = get_node_id()
        record_data["exc_info"] = None
        record_data["stack_info"] = None
        if record.exc_info:
            record_data["exc_text"] = format_exception(record.exc_info[1])
        else:
            record_data["exc_text"] = None
        record_data["args"] = ()
        if chain := getattr(record, "chain", None):
            record_data["msg"] = "[" + ":".join(chain) + "] " + record.getMessage()
        else:
            record_data["msg"] = record.getMessage()
        return orjson.dumps(record_data)

    def _process_pending_records(self) -> None:
        handler = cast(Callable[[bytes], None], self._handler)
        for record in self._pending_log_records:
            handler(record)
        self._pending_log_records.clear()

    def _async_udp_handler(self, record_data: bytes) -> None:
        cast(asyncio.DatagramTransport, self._transport).sendto(record_data)

    def _sync_udp_handler(self, record_data: bytes) -> None:
        cast(socket.SocketType, self._transport).send(record_data)

    def _switch_to_sync(self) -> None:
        if self._transport_scheme is self.TransportScheme.UDP:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((self._log_url.host, self._log_url.port))
            self._transport = sock
            self._handler = self._sync_udp_handler
        else:
            raise NotImplementedError("TCP sync endpoint is currently not implemented yet.")

    def _close_transport(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    async def async_cleanup(self) -> None:
        await super().async_cleanup()
        self._close_transport()
        self._switch_to_sync()

    async def async_init(self) -> None:
        await super().async_init()
        if self._transport_scheme is self.TransportScheme.UDP:
            self._transport, _ = await self.aio.loop.create_datagram_endpoint(
                asyncio.DatagramProtocol,
                remote_addr=(self._log_url.host, self._log_url.port)
            )
            self._handler = self._async_udp_handler
        else:
            raise NotImplementedError("TCP endpoint is currently not implemented yet.")
        cast(asyncio.Transport, self._transport).set_write_buffer_limits(high=2*16)
        if self._pending_log_records:
            self._process_pending_records()

    async def drain(self) -> None:
