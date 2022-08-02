import unittest

import fio_plugin
from arcaflow_plugin_sdk import plugin


class FioPluginTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            fio_plugin.FioParams(
                size='4m',
                # ioengine=AsyncIoEngine.libaio,
                ioengine='libaio',
                iodepth=32,
                # io_submit_mode=IoSubmitMode.offline,
                io_submit_mode='offline',
                rate_iops=50,
                # rate_process=RateProcess.poisson,
                rate_process='poisson',
                direct=True
            )
        )

        plugin.test_object_serialization(
            fio_plugin.FioSuccessOutput(
                'fio-3.29',
                1659468121,
                1659468121278,
                "Tue Aug  2 15:22:01 2022",
            )
        )

    def test_functional(self):
        params =  fio_plugin.FioParams(
                size='4m',
                # ioengine=AsyncIoEngine.libaio,
                ioengine='libaio',
                iodepth=32,
                # io_submit_mode=IoSubmitMode.offline,
                io_submit_mode='offline',
                rate_iops=50,
                # rate_process=RateProcess.poisson,
                rate_process='poisson',
                direct=True
            )

        output_id, output_data = fio_plugin.run(params)
        self.assertEqual('success', output_id)
        self.assertEqual(
            output_data,
            fio_plugin.FioSuccessOutput(
                'fio-3.29',
                1659468121,
                1659468121278,
                "Tue Aug  2 15:22:01 2022",
            )
        )



if __name__ == '__main__':
    unittest.main()