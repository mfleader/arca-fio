"""
Copyright 2022 Matthew F Leader
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

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
    name: str
    size: str
    ioengine: IoEngine
    iodepth: int
    rate_iops: int
    io_submit_mode: IoSubmitMode
    direct: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = 0
    atomic: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = 0
    buffered: typing.Annotated[Optional[int], validation.min(0), validation.max(1)] = 1
    readwrite: Optional[IoPattern] = IoPattern.read.value
    rate_process: Optional[RateProcess] = RateProcess.linear.value


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