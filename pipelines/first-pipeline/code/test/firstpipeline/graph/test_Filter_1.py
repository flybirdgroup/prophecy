from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from firstpipeline.graph.Filter_1 import *
from firstpipeline.config.ConfigStore import *


class Filter_1Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/firstpipeline/graph/Filter_1/in0/schema.json',
            'test/resources/data/firstpipeline/graph/Filter_1/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/firstpipeline/graph/Filter_1/out/schema.json',
            'test/resources/data/firstpipeline/graph/Filter_1/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = Filter_1(self.spark, dfIn0)
        assertDFEquals(dfOut.select("value"), dfOutComputed.select("value"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
