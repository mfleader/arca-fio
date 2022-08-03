import unittest
import json
import yaml

import fio_plugin
from arcaflow_plugin_sdk import plugin


class FioPluginTest(unittest.TestCase):
    with open('mocks/poisson-rate-submission_plus-output.json', 'r') as fout:
        poisson_submit_outfile = fout.read()

    poisson_submit_output = json.loads(poisson_submit_outfile)

    with open('mocks/poisson-rate-submission_input.yaml', 'r') as fin:
        poisson_submit_infile = fin.read()

    poisson_submit_input = yaml.load(poisson_submit_infile)


    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            fio_plugin.FioParams(
                poisson_submit_input
            )
        )

        plugin.test_object_serialization(
            fio_plugin.FioSuccessOutput(
                **poisson_submit_output
            )
        )

    # def test_functional(self):
    #     params = fio_plugin.FioParams(
    #             **poisson_submit_input
    #         )

    #     output_id, output_data = fio_plugin.run(params)
    #     self.assertEqual('success', output_id)
    #     self.assertEqual(
    #         output_data,
    #         fio_plugin.FioSuccessOutput(
    #             'fio-3.29',
    #             1659468121,
    #             1659468121278,
    #             "Tue Aug  2 15:22:01 2022",
    #         )
    #     )



if __name__ == '__main__':
    unittest.main()