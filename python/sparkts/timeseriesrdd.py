from py4j.java_gateway import java_import

class TimeSeriesRDD(object):
  
  def __init__(self, jtsrdd):
    self._jtsrdd = jtsrdd

  def collect_as_timeseries(self):
    jts = self._jtsrdd.collectAsTimeSeries()
    TimeSeries(jts) 
  
def time_series_rdd(dt_index, series_rdd):
  sc = series_rdd._ctx
  java_import(sc._jvm, "com.cloudera.sparkts.TimeSeriesRDD")
  jtsrdd = sc._jvm.TimeSeriesRDD.timeSeriesRDD(dt_index._jdt_index, series_rdd._jrdd)
  TimeSeriesRDD(jtsrdd)


