from py4j.java_gateway import java_import

class UniformDateTimeIndex:
  
    def __init__(self, jdt_index):
        self._jdt_index = jdt_index

class DayFrequency:
  
    def __init__(self, jfreq):
        self._jfreq = jfreq

    def __init__(self, days, sc):
        self._jfreq = sc._jvm.com.cloudera.sparkts.DayFrequency(days)

def uniform(start, periods, frequency, sc):
    jdt_index = sc._jvm.com.cloudera.sparkts.DateTimeIndex.uniform(start, periods, frequency._jfreq)
    UniformDateTimeIndex(jdt_index)

