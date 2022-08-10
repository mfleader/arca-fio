import sys
import typing
import json
import subprocess
import dataclasses
from traceback import format_exc
from typing import Union

from arcaflow_plugin_sdk import plugin
from fio_schema import FioParams, FioSuccessOutput, FioErrorOutput, fio_output_schema


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