This fork of scala-redis is meant to be used with the isbardel fork of redis ([[https://github.com/lsbardel/redis]]).  That branch of redis adds several operations on time series data.  For a description, look here:  https://github.com/lsbardel/redis/blob/quantredis/timeseries.rst.

The additional functionality provides efficient support for time series data.  All additional opertions are contained in the TimeSeriesOpertions trait which has been mixed in to the RedisCommand trait.  

The additional commands are similar to those in SortedSetOperations. Aside from the method names, the primary difference to a caller is that time is used for ranking.  Typical usage for financial data would include using the ticker symbol as the key, the timestamp of a record converted to a Double as the ranking, and the remainder of the record as the value.  For usage examples, review the tests contained in TimeSeriesOperationsSpec.  
