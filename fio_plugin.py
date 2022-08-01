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
import sys
import typing
import enum
import tempfile
import yaml
import json
import subprocess
import dataclasses
import fileinput
import os
import shutil
import csv
from dataclasses import dataclass
# import dataclasses
from typing import List, Optional, Union, Annotated
from arcaflow_plugin_sdk import plugin
from arcaflow_plugin_sdk import schema


# see https://fio.readthedocs.io/en/latest/fio_doc.html#i-o-type
IoPattern = enum.Enum(
    'read', 'write', 'randread', 'randwrite', 'randtrim',
    'rw', 'randrw', 'trimwrite'
)


# TODO: rw,readwrite union value?


# TODO: readwrite sequencer
# RwSequencer = enum.Enum(
#     'sequential', 'identical'
# )

UnifiedRwReport = enum.Enum(
    'none', 'mixed', 'both'
)

# TODO: add other ioengines?
# https://fio.readthedocs.io/en/latest/fio_doc.html#i-o-engine
IoEngine = enum.Enum(
    'sync', 'psync',
    'libaio',
    'windowsaio'
)

IoSubmitMode = enum.Enum(
    'inline', 'offload'
)

RateProcess: enum.Enum(
    'linear', 'poisson'
)





@dataclass
class IoType:
    atomic: Optional[bool] = False
    buffered: Optional[bool] = True
    direct: Optional[bool] = False
    readwrite: Optional[IoPattern] = 'read'
    # rw_sequencer = Optional[RwSequencer] =
    unified_rw_reporting: Optional[UnifiedRwReport] = 'none'
    randrepeat: Optional[bool] = True
    allrandrepeat: Optional[bool] = False
    # randseed: Optional[Union[int, None]] = None


@dataclass
class IoSize:
    size: str


@dataclass
class IoDepth:
    iodepth: int
    io_submit_mode: IoSubmitMode


@dataclass
class IoRate:
    rate_iops: int
    rate_process: Optional[RateProcess] = 'linear'


@dataclass
class FioParams2:
    iotype: IoType
    ioengine: IoEngine
    iodepth: IoDepth
    iorate: IoRate


@dataclass
class FioParams:
    size: str
    ioengine: IoEngine
    iodepth: int  # only used with async ioengines
    io_submit_mode: IoSubmitMode  # only used with async ioengines
    rate_iops: int
    rate_process: Optional[RateProcess] = 'linear'
    direct: Optional[bool] = False
    readwrite: Optional[IoPattern] = 'read'



# fio_input_schema = plugin.build_object_schema(FioParams)



# @dataclass
# class FioSuccessOutput:


# @dataclass
# class FioErrorOutput:
#     error: str


# @plugin.step(
#     id="workload",
#     name="fio workload",
#     description="run an fio workload",
#     outputs={"success": FioSuccessOutput, "error": FioErrorOutput}
# )


if __name__ == '__main__':
    fio_input_schema = plugin.build_object_schema(FioParams)
    print(fio_input_schema)