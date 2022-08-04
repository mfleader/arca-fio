import unittest
import json
import yaml

import fio_plugin
from arcaflow_plugin_sdk import plugin


with open('mocks/poisson-rate-submission_plus-output.json', 'r') as fout:
    poisson_submit_outfile = fout.read()

poisson_submit_output = json.loads(poisson_submit_outfile)

with open('mocks/poisson-rate-submission_input.yaml', 'r') as fin:
    poisson_submit_infile = fin.read()

poisson_submit_input = yaml.safe_load(poisson_submit_infile)


class FioPluginTest(unittest.TestCase):

    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            fio_plugin.FioParams(
                **poisson_submit_input
            )
        )

        plugin.test_object_serialization(
            fio_plugin.fio_output_schema.unserialize(
                poisson_submit_output
            )
        )

    def test_functional(self):
        params = fio_plugin.FioParams(
                **poisson_submit_input
            )

        output_id, output_data = fio_plugin.run(params)

        with open('fio-plus.json', 'r') as fio_output_file:
            fio_results = fio_output_file.read()
            output_actual: fio_plugin.FioSuccessOutput = fio_plugin.fio_output_schema.unserialize(
                json.loads(fio_results))

        self.assertEqual('success', output_id)
        self.assertEqual(
            output_data,
            output_actual
        )



if __name__ == '__main__':
    unittest.main()