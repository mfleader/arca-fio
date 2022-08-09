import sys
import typing
import enum
import json
import subprocess
import dataclasses
from traceback import format_exc
from dataclasses import dataclass, field
from typing import Optional, Union, Annotated, Dict

from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import schema

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
    # direct: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = 0
    # atomic: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = 0
    # buffered: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = 1
    direct: typing.Annotated[Optional[int], validation.min(0)] = field(
        default=0,
        metadata={
            'name': 'Direct',
            'description': ''
        }
    )
    atomic: typing.Annotated[Optional[int], validation.min(0)] = field(
        default=0,
        metadata={
            'name': 'Atomic',
            'description': ''
        }
    )
    buffered: typing.Annotated[Optional[int], validation.min(0)] = field(
        default=0,
        metadata={
            'name': 'Buffered',
            'description': ''
        }
    )
    readwrite: Optional[IoPattern] = field(
        default=IoPattern.read.value,
        metadata={
            'name': 'Read/Write',
            'description': ''
        }
    )
    rate_process: Optional[RateProcess] = field(
        default=RateProcess.linear.value,
        metadata={
            'name': 'Rate Process',
            'description': ''
        }
    )


@dataclass
class IoLatency:
    min_: int = field(metadata={"id": "min" })
    max_: int = field(metadata={"id": "max"})
    mean: float
    stddev: float
    N: int
    percentile: Optional[Dict[str, int]] = None
    bins: Optional[Dict[str, int]] = None


@dataclass
class SyncIoOutput:
    total_ios: int
    lat_ns: IoLatency


@dataclass
class AioOutput:
    io_bytes: int
    io_kbytes: int
    bw_bytes: int
    bw: int
    iops: float
    runtime: int
    total_ios: int
    short_ios: int
    drop_ios: int
    slat_ns: IoLatency
    clat_ns: IoLatency
    lat_ns: IoLatency
    bw_min: int
    bw_max: int
    bw_agg: float
    bw_mean: float
    bw_dev: float
    bw_samples: int
    iops_min: int
    iops_max: int
    iops_mean: float
    iops_stddev: float
    iops_samples: int


@dataclass
class JobResult:
    jobname: str
    groupid: int
    error: int
    eta: int
    elapsed: int
    job_options: Dict[str, str] = field(metadata={
        "id": "job options",
        "name": "job options"
    })
    read: AioOutput
    write: AioOutput
    trim: AioOutput
    sync: SyncIoOutput
    job_runtime: int
    usr_cpu: float
    sys_cpu: float
    ctx: int
    majf: int
    minf: int
    iodepth_level: Dict[str, float]
    iodepth_submit: Dict[str, float]
    iodepth_complete: Dict[str, float]
    latency_ns: Dict[str, float]
    latency_us: Dict[str, float]
    latency_ms: Dict[str, float]
    latency_depth: int
    latency_target: int
    latency_percentile: float
    latency_window: int


@dataclass
class DiskUtilization:
    name: str
    read_ios: int
    write_ios: int
    read_merges: int
    write_merges: int
    read_ticks: int
    write_ticks: int
    in_queue: int
    util: float
    aggr_read_ios: Optional[int] = None
    aggr_write_ios: Optional[int] = None
    aggr_read_merges: Optional[int] = None
    aggr_write_merge: Optional[int] = None
    aggr_read_ticks: Optional[int] = None
    aggr_write_ticks: Optional[int] = None
    aggr_in_queue: Optional[int]= None
    aggr_util: Optional[float] = None


@dataclass
class FioErrorOutput:
    error: str


fio_input_schema = plugin.build_object_schema(FioParams)
job_schema = plugin.build_object_schema(JobResult)


@dataclass
class FioSuccessOutput:
    fio_version: str = field(metadata={
        "id": "fio version",
        "name": "fio version"
    })
    timestamp: int
    timestamp_ms: int
    time: str
    jobs: typing.List[JobResult]
    global_options: Optional[Dict[str, str]] = field(
        default=None,
        metadata={
            "id": "global options",
            "name": "global options"
            }
    )
    disk_util: Optional[typing.List[DiskUtilization]] = None


fio_output_schema = plugin.build_object_schema(FioSuccessOutput)


@plugin.step(
    id="workload",
    name="fio workload",
    description="run an fio workload",
    outputs={"success": FioSuccessOutput, "error": FioErrorOutput}
)
def run(params: FioParams) -> typing.Tuple[str, Union[FioSuccessOutput, FioErrorOutput]]:
    try:
        outfile_name = 'fio-plus'
        cmd = [
            'fio',
            *[
                f"--{key}={str(value)}" for key, value in dataclasses.asdict(params).items()
            ],
            '--output-format=json+',
            f"--output={outfile_name}.json"
        ]

        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError:
            return 'error', FioErrorOutput(format_exc())

        with open(f'{outfile_name}.json', 'r') as output_file:
            fio_results = output_file.read()

        output: FioSuccessOutput = fio_output_schema.unserialize(json.loads(fio_results))
        return 'success', output
    except Exception:
        return 'oops', FioErrorOutput(format_exc())


if __name__ == '__main__':
    sys.exit(
        plugin.run(
            plugin.build_schema(
                run
            )
        )
    )