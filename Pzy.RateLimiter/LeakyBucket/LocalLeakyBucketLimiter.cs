namespace Pzy.RateLimiter.LeakyBucket
{
    /// <summary>
    /// Local leaky bucket limiter
    /// 
    /// Author: PZY
    /// Date: 2022-09-21
    /// 
    /// Reference links:
    /// https://pzy.io/posts/leaky-bucket-limiter-principle-algorithm/
    /// https://zhuanlan.zhihu.com/p/441005648
    /// https://github.com/mennanov/limiters/blob/master/leakybucket.go
    /// </summary>
    public class LocalLeakyBucketLimiter
    {
        /// <summary>
        /// the maximum allowed number of tockens in the bucket.
        /// </summary>
        private readonly int _bucketCapacity;

        /// <summary>
        /// the output rate: 1 request per the rate duration (in milliseconds).
        /// </summary>
        private readonly int _leakRate;

        /// <summary>
        /// the Unix timestamp in milliseconds of the most recent request.
        /// </summary>
        private long _lastTimestamp;

        private readonly SemaphoreSlim _semaphore;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bucketCapacity">Capacity is the maximum allowed number of tockens in the bucket.</param>
        /// <param name="leakRate">the output rate: 1 request per the rate duration (in milliseconds).</param>
        public LocalLeakyBucketLimiter(int bucketCapacity, int leakRate)
        {
            _bucketCapacity = bucketCapacity;
            _leakRate = leakRate;

            //Only allow one thread at a time in. 
            _semaphore = new SemaphoreSlim(1, 1);
        }

        /// <summary>
        /// Acquire returns the lock whether can be executed before the request can be processed.
        /// </summary>
        /// <param name="waitMilliseconds">the time duration (in milliseconds) to wait before the request can be processed.
        /// </param>
        /// <returns>It returns false if the the request overflows the bucket's capacity.
        /// In this case the returned duration means how long it would have taken to wait for the request to be processed 
        /// if the bucket was not overflowed.
        /// </returns>
        public bool Acquire(out long waitMilliseconds)
        {
            //Lock here to ensure that each request is processed in order
            _semaphore.Wait();

            try
            {
                //current time in milliseconds
                long nowTimestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                var executionTimestamp = EvaluateExectionTimestamp(nowTimestamp, _lastTimestamp);

                // Calculate if the bucket's capacity is full
                waitMilliseconds = executionTimestamp - nowTimestamp;
                if (waitMilliseconds >= _bucketCapacity * _leakRate)
                {
                    //Because the request is to be discarded here, all must keep the state before the new request is queued
                    return false;
                }

                _lastTimestamp = executionTimestamp;
                return true;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// Evaluation (current request) execution time
        /// </summary>
        /// <param name="nowTimestamp">current time</param>
        /// <param name="lastTimestamp">last exection time</param>
        private long EvaluateExectionTimestamp(long nowTimestamp, long lastTimestamp)
        {
            if (nowTimestamp < lastTimestamp)
            {
                // It means that there are already requests in the queue,
                // so the time for new requests to be processed after they come in and queue up is after the _leakRate
                lastTimestamp += _leakRate;
            }
            else
            {
                // Indicates that the bucket is empty, maybe the initial state, or maybe all requests have been processed.

                //How long to wait for the time to wait for the request to be processed
                long offset = 0;

                //Represents how long the current time has passed since the last time the request was processed
                long delta = nowTimestamp - lastTimestamp;

                if (delta < _leakRate)
                {
                    //Indicates that it is not yet time to process the next request, you need to wait for the offset
                    offset = _leakRate - delta;
                }
                //If delta >= _leakRate, indicates that the current time has exceeded the _leakRate time since the last time the request was processed,
                //the offset should be 0, and the new request should be processed immediately.

                //Update the time when the request should be processed
                lastTimestamp = nowTimestamp + offset;
            }

            return lastTimestamp;
        }
    }
}
