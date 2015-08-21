from test_utils import PySparkTestCase
from datetimeindex import *

class UniformDateTimeIndexTestCase(PySparkTestCase):
  def test_uniform_instantiators(self):
    freq = DayFrequency(3, self.sc)

