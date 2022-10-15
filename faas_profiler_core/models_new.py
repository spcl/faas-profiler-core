
import serpyco

from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple, List, Any
from datetime import datetime
from uuid import UUID
from socket import AddressFamily

from .utilis import sec_to_ms
from .constants import (
    Operation,
    RecordDataType,
    Provider,
    Runtime,
    Service,
    TriggerSynchronicity,
    UnidentifiedOperation,
    UnidentifiedService,
    operation_proxy,
    service_proxy,
    TRACE_ID_HEADER,
    RECORD_ID_HEADER,
    PARENT_ID_HEADER
)

UNIDENTIFIED = "unidentified"

FUNCTION_KEY_DELLIMITER = "::"
KEY_VALUE_DELIMITER = "#"
IDENTIFIER_DELIMITER = 2 * KEY_VALUE_DELIMITER


@dataclass
class BaseModel(serpyco.SerializerMixin):
    """
    Base model
    """
    pass


@dataclass
class Test(BaseModel):
    # __slots__ = ("a", "b")
    a: int
    b: int


@dataclass
class FunctionContext(BaseModel):
    """
    Context definition for serverless functions.
    """
    # __slots__ = (
    #     "provider",
    #     "runtime",
    #     "runtime",
    #     "function_name",
    #     "handler",
    #     "invoked_at",
    #     "handler_executed_at",
    #     "handler_finished_at",
    #     "finished_at",
    #     "max_memory",
    #     "max_execution_time",
    #     "has_error",
    #     "error_type",
    #     "error_message",
    #     "response")

    # Function Key
    provider: Provider = Provider.UNIDENTIFIED
    runtime: Runtime = Runtime.UNIDENTIFIED
    region: str = UNIDENTIFIED
    function_name: str = UNIDENTIFIED

    handler: Optional[str] = UNIDENTIFIED

    # Timing Properties
    invoked_at: Optional[datetime] = None
    handler_executed_at: Optional[datetime] = None
    handler_finished_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    max_memory: Optional[int] = None
    max_execution_time: Optional[int] = None

    # Error Evidence
    has_error: Optional[bool] = False
    error_type: Optional[str] = None
    error_message: Optional[str] = None

    # Payload and Response
    response: Optional[Any] = None

    # traceback: List[str] = field(default_factory=list)
    # arguments: dict = field(default_factory=dict)
    # environment_variables: dict = field(default_factory=dict)

    @property
    def function_key(self) -> str:
        """
        Returns a unique key for the function.
        """
        return FUNCTION_KEY_DELLIMITER.join(
            [self.provider.value, self.function_name])

    @property
    def handler_execution_time(self) -> float:
        """
        Returns the handler execution time in ms
        """
        if not (self.handler_executed_at or self.handler_finished_at):
            return

        delta = self.handler_finished_at - self.handler_executed_at
        return sec_to_ms(delta.total_seconds())

    @property
    def total_execution_time(self) -> float:
        """
        Returns the total executuon time in ms
        """
        if not (self.invoked_at or self.finished_at):
            return

        delta = self.finished_at - self.invoked_at
        return sec_to_ms(delta.total_seconds())

    @property
    def profiler_time(self) -> float:
        """
        Returns the total time for the profiler (overhead)
        """
        if not (self.total_execution_time or self.handler_execution_time):
            return

        return self.total_execution_time - self.handler_execution_time


@dataclass
class TracingContext(BaseModel):
    """
    Context definition for tracing
    """
    # __slots__ = ("trace_id", "record_id", "parent_id")

    trace_id: UUID
    record_id: UUID
    parent_id: Optional[UUID] = None

    def __str__(self) -> str:
        """
        Return tracing as string
        """
        return "TracingContext: TraceID={trace_id}, RecordID={record_id}, ParentID={parent_id}".format(
            trace_id=self.trace_id, record_id=self.record_id, parent_id=self.parent_id)

    @property
    def is_defined(self) -> bool:
        """
        Returns True iff trace ID and record ID is not None
        """
        return self.trace_id is not None and self.record_id is not None

    def to_injectable(self) -> dict:
        """
        Returns the context as injectable context.
        """
        ctx = {}
        if self.trace_id:
            ctx[TRACE_ID_HEADER] = str(self.trace_id)
        if self.record_id:
            ctx[RECORD_ID_HEADER] = str(self.record_id)
        if self.parent_id:
            ctx[PARENT_ID_HEADER] = str(self.parent_id)

        return ctx


@dataclass
class RequestContext(BaseModel):
    """
    Base class for inbound and outbound context.
    """
    # __slots__ = (
    #     "provider",
    #     "service",
    #     "operation",
    #     "trigger_synchronicity",
    #     "identifier",
    #     "tags",
    #     "latency")

    provider: Provider = Provider.UNIDENTIFIED
    service: Service = UnidentifiedService.UNIDENTIFIED
    operation: Operation = UnidentifiedOperation.UNIDENTIFIED

    trigger_synchronicity: TriggerSynchronicity = TriggerSynchronicity.UNIDENTIFIED

    identifier: Optional[Dict[str, str]] = field(default_factory=dict)
    tags: Optional[Dict[str, str]] = field(default_factory=dict)

    latency: Optional[float] = None

    def __str__(self) -> str:
        """
        Return request context as string
        """
        return "RequestContext: Provider={provider}, Service={service}, Operation={operation}, identifier={identifier}".format(
            provider=self.provider, service=self.service, operation=self.operation, identifier=self.identifier_string)

    @property
    def short_str(self) -> str:
        """
        Returns a short label for this context.
        """
        return "{service}::{operation}".format(
            service=self.service.name,
            operation=self.operation.value)

    def set_identifiers(self, identifiers: dict) -> None:
        """
        Merges identifier into stored identifier
        """
        if any(KEY_VALUE_DELIMITER in k for k in identifiers.keys()):
            raise ValueError(
                f"Keys of identifier dict must not contain any '{KEY_VALUE_DELIMITER}'.")

        if any(KEY_VALUE_DELIMITER in v for v in identifiers.values()):
            raise ValueError(
                f"Values of identifier dict must not contain any '{KEY_VALUE_DELIMITER}'.")

        self.identifier.update(identifiers)

    def set_tags(self, tags: dict) -> None:
        """
        Merges tags into stored tags
        """
        self.tags.update(tags)

    @property
    def identifier_string(self) -> str:
        """
        Returns the identifiers as string.
        """
        _identifier = {str(k): str(v) for k, v in self.identifier.items()}
        _identifier = sorted(
            _identifier.items(),
            key=lambda x: x[0],
            reverse=False)
        _identifier = map(
            lambda id: KEY_VALUE_DELIMITER.join(id),
            _identifier)

        return IDENTIFIER_DELIMITER.join(_identifier)

    @property
    def resolvable(self) -> bool:
        """
        Returns True if request context is resolvable.
        This is, if identifier are set.
        """
        return self.identifier is not None and self.identifier != {}

    @property
    def is_defined(self) -> bool:
        """
        Returns True if provider and service is not undefined
        """
        return (self.provider != Provider.UNIDENTIFIED
                and self.operation.value != UNIDENTIFIED)


@dataclass
class InboundContext(RequestContext):
    """
    Context definition for inbound requests
    """
    invoked_at: Optional[datetime] = None


@dataclass
class OutboundContext(RequestContext):
    """
    Context definition for outbound requests
    """
    called_at: Optional[datetime] = None
    returned_at: Optional[datetime] = None

    has_error: bool = False
    error_message: str = None


"""
Record data
"""


@dataclass
class RecordData(BaseModel):
    name: str
    type: RecordDataType
    results: Any


@dataclass
class TraceRecord(BaseModel):
    function_context: FunctionContext
    tracing_context: TracingContext

    inbound_context: Optional[InboundContext]
    outbound_contexts: List[OutboundContext] = field(default_factory=list)

    data: Dict[str, RecordData] = field(default_factory=dict)

    """
    Record shortcut properties
    """

    def __str__(self) -> str:
        record_str = self.record_name

        if self.record_id:
            record_str += f" - {str(self.record_id)[:8]}"

        if self.total_execution_time:
            record_str += " - ({:.2f} ms)".format(self.total_execution_time)

        return record_str

    @property
    def node_label(self) -> str:
        """
        Returns a label for this node.
        """
        _exe_time = "N/A"
        if self.total_execution_time:
            _exe_time = "{:.2f} ms".format(self.total_execution_time)

        return "{name} [{record_id}] \n ({exe_time})".format(
            name=self.record_name,
            record_id=str(self.record_id)[:8],
            exe_time=_exe_time)

    @property
    def function_key(self) -> str:
        """
        Returns the function key of the function context.
        """
        return self.function_context.function_key

    @property
    def trace_id(self):
        """
        Returns the trace id.
        """
        return self.tracing_context.trace_id

    @property
    def record_id(self):
        """
        Returns the record id.
        """
        return self.tracing_context.record_id

    @property
    def record_name(self):
        """
        Returns the record name, composed of provider and function name
        """
        func_ctx = self.function_context
        if not func_ctx:
            return FUNCTION_KEY_DELLIMITER.join([
                Provider.UNIDENTIFIED.value,
                UNIDENTIFIED])

        return f"{func_ctx.provider.value}::{func_ctx.function_name}"

    @property
    def total_execution_time(self):
        """
        Returns the execution time of the trace in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.total_execution_time

    @property
    def overhead_time(self):
        """
        Returns the overhead time spent by the profiler in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.profiler_time

    @property
    def handler_execution_time(self):
        """
        Returns the execution time of the handler in ms.
        """
        if self.function_context is None:
            return

        return self.function_context.handler_execution_time


"""
Memory Measurement
"""


@dataclass
class MemoryLineUsageItem(BaseModel):
    line_number: int
    content: str
    occurrences: int
    memory_increment: float
    memory_total: float


@dataclass
class MemoryLineUsage(BaseModel):
    line_memories: List[MemoryLineUsageItem] = field(
        default_factory=list)


@dataclass
class MemoryUsage(BaseModel):
    interval: float
    rss_baseline: float = 0.0
    vms_baseline: float = 0.0
    rss: List[Tuple[float, float]] = field(default_factory=list)
    vms: List[Tuple[float, float]] = field(default_factory=list)


"""
CPU Measurement
"""


@dataclass
class CPUUsage(BaseModel):
    interval: float = None
    percentage: List[Tuple[float, float]] = field(default_factory=list)


@dataclass
class CPUCoreUsage(BaseModel):
    interval: float = None
    percentage: Dict[int, List[Tuple[float, float]]
                     ] = field(default_factory=dict)


"""
Network Measurement
"""


@dataclass
class NetworkConnectionItem(BaseModel):
    socket_family: AddressFamily
    remote_address: str
    number_of_connections: int


@dataclass
class NetworkConnections(BaseModel):
    connections: List[NetworkConnectionItem] = field(
        default_factory=list)


@dataclass
class NetworkIOCounters(BaseModel):
    bytes_sent: int
    bytes_received: int
    packets_sent: int
    packets_received: int
    error_in: int
    error_out: int
    drop_in: int
    drop_out: int


"""
Disk Measurement
"""


@dataclass
class DiskIOCounters(BaseModel):
    read_count: int
    write_count: int
    read_bytes: int
    write_bytes: int


"""
Informations
"""


@dataclass
class InformationEnvironment(BaseModel):
    runtime_name: str = UNIDENTIFIED
    runtime_version: str = UNIDENTIFIED
    runtime_implementation: str = UNIDENTIFIED
    runtime_compiler: str = UNIDENTIFIED
    byte_order: str = UNIDENTIFIED
    platform: str = UNIDENTIFIED
    interpreter_path: str = UNIDENTIFIED
    packages: List[str] = field(default_factory=list)


@dataclass
class InformationOperatingSystem(BaseModel):
    boot_time: datetime
    system: str = UNIDENTIFIED
    node_name: str = UNIDENTIFIED
    release: str = UNIDENTIFIED
    machine: str = UNIDENTIFIED


@dataclass
class InformationIsWarm(BaseModel):
    is_warm: bool = False
    warm_since: datetime = UNIDENTIFIED
    warm_for: int = 0


@dataclass
class InformationTimeShift(BaseModel):
    server: str = UNIDENTIFIED
    offset: float = 0.0


"""
Captures
"""


@dataclass
class S3AccessItem(BaseModel):
    mode: str = None
    bucket_name: str = None
    object_key: str = None
    object_size: float = None
    execution_time: float = None


@dataclass
class S3Accesses(BaseModel):
    accesses: List[S3AccessItem] = field(
        default_factory=list)


@dataclass
class EFSAccessItem(BaseModel):
    mode: str = None
    file: str = None
    file_size: float = None
    execution_time: float = None


@dataclass
class EFSAccesses(BaseModel):
    mount_point: str
    accesses: List[EFSAccessItem] = field(
        default_factory=list)
