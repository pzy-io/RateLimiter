namespace Pzy.RateLimiter.LeakyBucket
{
    internal class OtherLeakyBucketLimiter
    {
        /// <summary>
        /// the maximum allowed number of tockens in the bucket.
        /// </summary>
        private readonly int _bucketCapacity;

        /// <summary>
        /// the output rate: requests per second
        /// </summary>
        private readonly int _leakRate;

        /// <summary>
        /// the Unix timestamp in milliseconds of the last request.
        /// </summary>
        private long _lastUpdateTime;

        /// <summary>
        /// remaining water
        /// </summary>
        private volatile int _water;

        /// <summary>
        /// thread lock
        /// </summary>
        private readonly SemaphoreSlim _semaphore;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bucketCapacity">Capacity is the maximum allowed number of tockens in the bucket.</param>
        /// <param name="leakRate">requests per second. 
        /// Special inputs are not considered for now, such as scenarios where leakRate is less than 1.
        /// </param>
        internal OtherLeakyBucketLimiter(int bucketCapacity, int leakRate)
        {
            //Only allow one thread at a time in. 
            _semaphore = new SemaphoreSlim(1, 1);

            _bucketCapacity = bucketCapacity;
            _leakRate = leakRate;
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
        public bool Acquire()
        {
            _semaphore.Wait();

            try
            {
                //current time in milliseconds
                long nowTimestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
                long executionTimestamp = nowTimestamp - _lastUpdateTime;

                int leakyWater = (int)(executionTimestamp * 1.0 * _leakRate / 1000);
                _water = Math.Max(0, _water - leakyWater);

                //If the water leakage is small, return it directly. Avoid starting when a lot of traffic goes through.
                if (leakyWater < 1)
                {
                    return false;
                }

                if ((_water + 1) < _bucketCapacity)
                {
                    //Try to add water, and the bucket is not full yet
                    _water++;
                    _lastUpdateTime = nowTimestamp;
                    return true;
                }
                else
                {
                    //Bucket is full, refuse to add water
                    return false;
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}
