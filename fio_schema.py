import typing
import enum
from dataclasses import dataclass, field
from typing import Optional, Annotated, Dict

from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import validation


class IoPattern(str, enum.Enum):
    read = 'read'
    write = 'write'
    randread = 'randread'
    randwrite = 'randwrite'
    rw = 'rw'
    readwrite = 'readwrite'
    randrw = 'randrw'
    trimwrite = 'trimwrite'
    randtrim = 'randtrim'

    def __str__(self) -> str:
        return self.value


class RateProcess(str, enum.Enum):
    linear = 'linear'
    poisson = 'poisson'

    def __str__(self) -> str:
        return self.value


class IoSubmitMode(str, enum.Enum):
    inline = 'inline'
    offload = 'offload'

    def __str__(self) -> str:
        return self.value


class IoEngine(str, enum.Enum):
    _sync_io_engines = {
        'sync', 'psync'
    }
    _async_io_engines = {
        'libaio', 'windowsaio'
    }
    sync = 'sync'
    psync = 'psync'
    libaio = 'libaio'
    windowsaio = 'windowsaio'

    def __str__(self) -> str:
        return self.value

    def is_sync(self) -> bool:
        return self.value in self._sync_io_engines


@dataclass
class FioParams:
    name: Annotated[str, validation.min(1)] = field(metadata={
        "name": 'name',
        'description': 'the name of the fio job'
    })
    size: Annotated[str, validation.min(2)] = field(metadata={
        "name": 'size',
        'description': (
            """The total size in bytes of the file IO for each thread of this job."""
            """If a unit other than bytes is used, the integer is concatenated with """
            """the corresponding unit abbreviation (i.e. 10KiB, 10MiB, 10GiB, ...)"""
        )
    })
    ioengine: IoEngine = field(metadata={
        "name": "IO Engine",
        "description": "Defines how the job issues IO to the file."
    })
    iodepth: int = field(metadata={
        "name": "IO Depth",
        "description": "number of IO units to keep in flight against the file."
    })
    rate_iops: int = field(metadata={
        "name": 'IOPS Cap',
        "description": 'maximum allowed rate of IO operations per second'
    })
    io_submit_mode: IoSubmitMode = field(metadata={
        "name": "IO Submit Mode",
        "description": "Controls how fio submits IO to the IO engine."
    })
    direct: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = field(
        default=0,
        metadata={
            'name': 'Direct',
            'description': 'non-buffered IO'
        }
    )
    atomic: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = field(
        default=0,
        metadata={
            'name': 'Atomic',
            'description': 'attemp to use atomic direct IO'
        }
    )
    buffered: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = field(
        default=1,
        metadata={
            'name': 'Buffered',
            'description': 'use buffered IO'
        }
    )
    readwrite: Optional[IoPattern] = field(
        default=IoPattern.read.value,
        metadata={
            'name': 'Read/Write',
            'description': 'type of IO pattern'
        }
    )
    rate_process: Optional[RateProcess] = field(
        default=RateProcess.linear.value,
        metadata={
            'name': 'Rate Process',
            'description': 'Controls the distribution of delay between IO submissions.'
        }
    )


# @dataclass
# class FioParams:
#     jobs: typing.List[JobParams]


@dataclass
class IoLatency:
    min_: int = field(metadata={
        "id": "min",
        "name": "IO Latency Min",
        "description": "IO latency minimum"
    })
    max_: int = field(metadata={
        "id": "max",
        "name": "IO Latency Max",
        "description": "IO latency maximum"
    })
    mean: float = field(metadata={
        "name": "IO Latency Mean",
        "description": "IO latency mean"
    })
    stddev: float = field(metadata={
        "name": "IO Latency StdDev",
        "description": "IO latency standard deviation"
    })
    N: int = field(metadata={
        "name": "IO Latency Sample Quantity",
        "description": "quantity of IO latency samples collected"
    })
    percentile: Optional[Dict[str, int]] = field(
        default=None,
        metadata={
            "name": "IO Latency Cumulative Distribution",
            "description": "Cumulative distribution of IO latency sample"
        }
    )
    bins: Optional[Dict[str, int]] = field(
        default=None,
        metadata={
            "name": "Binned IO Latency Sample",
            "description": "binned version of the IO latencies collected"
        }
    )


@dataclass
class SyncIoOutput:
    total_ios: int = field(metadata={
        "name": "Quantity of Latencies Logged",
        "description": "Quantity of latency samples collected (i.e. logged)."
    })
    lat_ns: IoLatency = field(metadata={
        "name": "Latency ns",
        "description": "Total latency in nanoseconds."
    })


@dataclass
class AioOutput:
    io_bytes: int = field(metadata={
        "name": "IO B",
        "description": "Quantity of IO transactions in bytes"
    })
    io_kbytes: int = field(metadata={
        "name": "IO KiB",
        "description": "Quantity of IO transactions in kibibytes"
    })
    bw_bytes: int = field(metadata={
        "name": "Bandwidth B",
        "description": "IO bandwidth used in bytes"
    })
    bw: int = field(metadata={
        "name": "Bandwidth KiB",
        "description": "IO bandwidth used in kibibytes"
    })
    iops: float = field(metadata={
        "name": "IOPS",
        "description": "IO operations per second"
    })
    runtime: int = field(metadata={
        "name": "Runtime",
        "description": "Length of time in seconds on this IO pattern type"
    })
    total_ios: int = field(metadata={
        "name": "Quantity of Latencies Logged",
        "description": "Quantity of latency samples collected (i.e. logged)"
    })
    short_ios: int = field(metadata={
        "name": "",
        "description": ""
    })
    drop_ios: int = field(metadata={
        "name": "",
        "description": ""
    })
    slat_ns: IoLatency = field(metadata={
        "name": "Submission Latency ns",
        "description": "Submission latency in nanoseconds"
    })
    clat_ns: IoLatency = field(metadata={
        "name": "Completion Latency ns",
        "description": "Completion latency in nanoseconds"
    })
    lat_ns: IoLatency = field(metadata={
        "name": "Latency ns",
        "description": "Total latency in nanoseconds."
    })
    bw_min: int = field(metadata={
        "name": "Bandwidth Min",
        "description": "Bandwidth minimum"
    })
    bw_max: int = field(metadata={
        "name": "Bandwidth Max",
        "description": "Bandwidth maximum"
    })
    bw_agg: float = field(metadata={
        "name": "Bandwidth Aggregate Percentile",
        "description": ""
    })
    bw_mean: float = field(metadata={
        "name": "Bandwidth Mean",
        "description": "Bandwidth mean"
    })
    bw_dev: float = field(metadata={
        "name": "Bandwidth Std Dev",
        "description": "Bandwidth standard deviation"
    })
    bw_samples: int = field(metadata={
        "name": "Bandwidth Sample Quantity",
        "description": "Quantity of bandwidth samples collected"
    })
    iops_min: int = field(metadata={
        "name": "IOPS Min",
        "description": "IO operations per second minimum"
    })
    iops_max: int = field(metadata={
        "name": "IOPS Max",
        "description": "IO operations per second maximum"
    })
    iops_mean: float = field(metadata={
        "name": "IOPS Mean",
        "description": "IO operations per second mean"
    })
    iops_stddev: float = field(metadata={
        "name": "IOPS Std Dev",
        "description": "IO operations per second standard deviation"
    })
    iops_samples: int = field(metadata={
        "name": "IOPS Sample Quantity",
        "description": "Quantity of IOPS samples collected"
    })


@dataclass
class JobResult:
    jobname: str = field(metadata={
        "name": "Job Name",
        "description": "Name of the job configuration"
    })
    groupid: int = field(metadata={
        "name": "Thread Group ID",
        "description": "Identifying number for thread group used in a job."
    })
    error: int = field(metadata={
        "name": "Error",
        "description": "An error code thrown by the job."
    })
    eta: int = field(metadata={
        "name": "ETA",
        "description": "Specifies when real-time estimates should be printed."
    })
    elapsed: int = field(metadata={
        "name": "",
        "description": ""
    })
    job_options: Dict[str, str] = field(metadata={
        "id": "job options",
        "name": "job options"
    })
    read: AioOutput = field(metadata={
        "name": "",
        "description": ""
    })
    write: AioOutput = field(metadata={
        "name": "",
        "description": ""
    })
    trim: AioOutput = field(metadata={
        "name": "",
        "description": ""
    })
    sync: SyncIoOutput = field(metadata={
        "name": "",
        "description": ""
    })
    job_runtime: int = field(metadata={
        "name": "",
        "description": ""
    })
    usr_cpu: float = field(metadata={
        "name": "",
        "description": ""
    })
    sys_cpu: float = field(metadata={
        "name": "",
        "description": ""
    })
    ctx: int = field(metadata={
        "name": "",
        "description": ""
    })
    majf: int = field(metadata={
        "name": "",
        "description": ""
    })
    minf: int = field(metadata={
        "name": "",
        "description": ""
    })
    iodepth_level: Dict[str, float] = field(metadata={
        "name": "",
        "description": ""
    })
    iodepth_submit: Dict[str, float] = field(metadata={
        "name": "",
        "description": ""
    })
    iodepth_complete: Dict[str, float] = field(metadata={
        "name": "",
        "description": ""
    })
    latency_ns: Dict[str, float] = field(metadata={
        "name": "",
        "description": ""
    })
    latency_us: Dict[str, float] = field(metadata={
        "name": "",
        "description": ""
    })
    latency_ms: Dict[str, float] = field(metadata={
        "name": "",
        "description": ""
    })
    latency_depth: int = field(metadata={
        "name": "",
        "description": ""
    })
    latency_target: int = field(metadata={
        "name": "",
        "description": ""
    })
    latency_percentile: float = field(metadata={
        "name": "",
        "description": ""
    })
    latency_window: int = field(metadata={
        "name": "",
        "description": ""
    })


@dataclass
class DiskUtilization:
    name: str = field(metadata={
        "name": "",
        "description": ""
    })
    read_ios: int = field(metadata={
        "name": "",
        "description": ""
    })
    write_ios: int = field(metadata={
        "name": "",
        "description": ""
    })
    read_merges: int = field(metadata={
        "name": "",
        "description": ""
    })
    write_merges: int = field(metadata={
        "name": "",
        "description": ""
    })
    read_ticks: int = field(metadata={
        "name": "",
        "description": ""
    })
    write_ticks: int = field(metadata={
        "name": "",
        "description": ""
    })
    in_queue: int = field(metadata={
        "name": "",
        "description": ""
    })
    util: float = field(metadata={
        "name": "",
        "description": ""
    })
    aggr_read_ios: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_write_ios: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_read_merges: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_write_merge: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_read_ticks: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_write_ticks: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_in_queue: Optional[int] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })
    aggr_util: Optional[float] = field(
        default=None,
        metadata={
            "name": "",
            "description": ""
    })


@dataclass
class FioErrorOutput:
    error: str = field(
        metadata={
            "name": "Job Error Traceback",
            "description": "Fio job traceback for debugging"
    })


fio_input_schema = plugin.build_object_schema(FioParams)
job_schema = plugin.build_object_schema(JobResult)


@dataclass
class FioSuccessOutput:
    fio_version: str = field(metadata={
        "id": "fio version",
        "name": "Fio version",
        "description": "Fio version used on job"
    })
    timestamp: int = field(metadata={
        "name": "Timestamp",
        "description": "POSIX compliant timestamp in seconds"
    })
    timestamp_ms: int = field(metadata={
        "name": "timestamp ms",
        "description": "POSIX compliant timestamp in milliseconds"
    })
    time: str = field(metadata={
        "name": "Time",
        "description": "Human readable datetime string"
    })
    jobs: typing.List[JobResult] = field(metadata={
        "name": "Jobs",
        "description": "List of job input parameter configurations"
    })
    global_options: Optional[Dict[str, str]] = field(
        default=None,
        metadata={
            "id": "global options",
            "name": "global options",
            "description": "Options applied to every job"
    })
    disk_util: Optional[typing.List[DiskUtilization]] = field(
        default=None,
        metadata={
        "name": "Disk Utlization",
        "description": "Disk utilization during job"
    })


fio_output_schema = plugin.build_object_schema(FioSuccessOutput)
