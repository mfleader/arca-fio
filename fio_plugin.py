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

import re
from readline import read_history_file
import sys
import typing
import enum
import tempfile
import yaml
import json
import subprocess
import fileinput
import os
import shutil
import csv
from dataclasses import dataclass, field
# import dataclasses
from typing import List, Optional, Union, Annotated
from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import schema


# see https://fio.readthedocs.io/en/latest/fio_doc.html#i-o-type
IoPattern = enum.Enum(
    'IoPattern',
    ' '.join((
        'read', 'write', 'randread', 'randwrite', 'randtrim',
        'rw', 'randrw', 'trimwrite'
    ))
)


# TODO: rw,readwrite union value?


# TODO: readwrite sequencer
# RwSequencer = enum.Enum(
#     'sequential', 'identical'
# )

UnifiedRwReport = enum.Enum(
    'UnifiedRwReport',
    ' '.join(('none', 'mixed', 'both'))
)


sync_io_engines = set(('sync', 'psync'))

SyncIoEngine = enum.Enum(
    'SyncIoEngine',
    list(sync_io_engines)
)

async_io_engines = set(('libaio', 'windowsaio'))

AsyncIoEngine = enum.Enum(
    'AsyncIoEngine',
    list(async_io_engines)
)

# TODO: add other ioengines?
# https://fio.readthedocs.io/en/latest/fio_doc.html#i-o-engine
IoEngine = Union[SyncIoEngine, AsyncIoEngine]




IoSubmitMode = enum.Enum(
    'IoSubmitMode',
    ' '.join(('inline', 'offload'))
)

RateProcess = enum.Enum(
    'RateProcess',
    ' '.join(('linear', 'poisson'))
)


@dataclass
class FioErrorOutput:
    error: str


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
    aggr_util: Optional[int] = None


# @dataclass
# class IoType:
#     atomic: Optional[bool] = False
#     buffered: Optional[bool] = True
#     direct: Optional[bool] = False
#     readwrite: Optional[IoPattern] = 'read'
#     # rw_sequencer = Optional[RwSequencer] =
#     unified_rw_reporting: Optional[UnifiedRwReport] = 'none'
#     randrepeat: Optional[bool] = True
#     allrandrepeat: Optional[bool] = False
#     # randseed: Optional[Union[int, None]] = None


# @dataclass
# class IoSize:
#     size: str


# @dataclass
# class IoDepth:
#     iodepth: int
#     io_submit_mode: IoSubmitMode


# @dataclass
# class IoRate:
#     rate_iops: int
#     rate_process: Optional[RateProcess] = 'linear'


# @dataclass
# class FioParams2:
#     iotype: IoType
#     ioengine: IoEngine
#     iodepth: IoDepth
#     iorate: IoRate

import dataclasses

@dataclass
class FioParams:
    name: str
    size: str
    # ioengine_str: str
    # ioengine: IoEngine = field(init=False)
    ioengine: str
    iodepth: int  # only used with async ioengines
    # io_submit_mode: IoSubmitMode  # only used with async ioengines
    io_submit_mode: str
    rate_iops: int
    # rate_process: Optional[RateProcess] = 'linear'
    rate_process: Optional[str] = 'linear'
    # direct: Optional[bool] = False
    # direct: Annotated[Optional[int], validation.max(1)] = 0
    direct: Optional[int] = 0
    # readwrite: Optional[IoPattern] = 'read'
    readwrite: Optional[str] = 'read'

    # def __post_init__(self):
    #     self.ioengine = IoEngine(self.ioengine_str)



# @dataclass
# class JobMetadata:
#     jobname: str
#     groupid: int
#     error: int
#     eta: int
#     elapsed: int
#     job_options: dict

#     @classmethod
#     def new(cls, json_data: dict):
#         kwargs: dict[str, typing.Any] = {}
#         for key, value in json_data.items():
#             safe_key = key.replace(' ', '_')
#             kwargs[safe_key] = value
#         return cls(**kwargs)


@dataclass
class IoLatency:
    min_: int = field(metadata={"id": "min" })
    max_: int = field(metadata={"id": "max"})
    mean: float
    stddev: float
    N: int
    percentile: Optional[dict[str, int]]
    bins: Optional[dict[str, int]]


@dataclass
class IoOutput:
    io_bytes: int
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
    # metadata: JobMetadata
    jobname: str
    groupid: int
    error: int
    eta: int
    elapsed: int
    job_options: dict[str, str]
    read: Optional[IoOutput]
    write: Optional[IoOutput]
    trim: Optional[IoOutput]
    sync: Optional[IoOutput]
    mixed: Optional[IoOutput]
    job_runtime: int
    usr_cpu: float
    sys_cpu: float
    ctx: int
    majf: int
    minf: int
    iodepth_level: dict[str, float]
    iodepth_submit: dict[str, float]
    iodepth_complete: dict[str, float]
    latency_ns: dict[str, float]
    latency_us: dict[str, float]
    latency_ms: dict[str, float]
    latency_depth: int
    latency_target: int
    latency_percentile: float
    latency_window: int

    @classmethod
    def new(cls, json_data: dict):
        kwargs: dict[str, typing.Any] = {}
        for key, value in json_data.items():
            safe_key = key.replace(' ', '_')
            kwargs[safe_key] = value
        return cls(**kwargs)


@dataclass
class FioSuccessOutput:
    fio_version: str = field(metadata={
        "id": "fio version",
        "name": "fio version"

    })
    timestamp: int
    timestamp_ms: int
    time: str
    jobs: List[JobResult]
    # global_options: Optional[dict[str, str]] = None
    # disk_util: Optional[List[DiskUtilization]] = None

    @classmethod
    def new(cls, json_data: dict):
        kwargs: dict[str, typing.Any] = {}
        for key, value in json_data.items():
            safe_key = key.replace(' ', '_')
            kwargs[safe_key] = value
        return cls(**kwargs)



fio_input_schema = plugin.build_object_schema(FioParams)
fio_output_schema = plugin.build_object_schema(FioSuccessOutput)


@plugin.step(
    id="workload",
    name="fio workload",
    description="run an fio workload",
    outputs={"success": FioSuccessOutput, "error": FioErrorOutput}
)
def run(params: FioParams) -> typing.Tuple[str, Union[FioSuccessOutput, FioErrorOutput]]:

    outfile_name = 'fio-plus'
    cmd = [
        'fio',
        *[
            f"--{key}={value}" for key, value in dataclasses.asdict(params).items()
        ],
        '--output-format=json+',
        f"--output=tmp/{outfile_name}.json"
    ]

    try:
        out = subprocess.check_output(
            cmd
        )
    except subprocess.CalledProcessError as error:
        return 'error', FioErrorOutput('oops')

    with open(f'tmp/{outfile_name}.json', 'r') as output_file:
        fio_results = output_file.read()

    fio_json = json.loads(fio_results)

    output = FioSuccessOutput.new(fio_json)

    return 'success', output


if __name__ == '__main__':
    sys.exit(
        plugin.run(
            plugin.build_schema(
                run
            )
        )
    )
    # FioParams(
    #             size='4m',
    #             # ioengine=AsyncIoEngine.libaio,
    #             ioengine='libaio',
    #             iodepth=32,
    #             # io_submit_mode=IoSubmitMode.offline,
    #             io_submit_mode='offline',
    #             rate_iops=50,
    #             # rate_process=RateProcess.poisson,
    #             rate_process='poisson',
    #             direct=True
    #         )

