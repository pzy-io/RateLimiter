using Pzy.RateLimiter.LeakyBucket;
using Xunit;

namespace Pzy.RateLimiter.Tests
{
    public class LeakyBucket_UnitTest
    {
        private const int bucketCapacity = 2;

        [Fact]
        public void Limit_Rate_500_Two_Times_Success_Test()
        {
            int leakRate = 500;
            var localLeakyBucketLimiter = new LocalLeakyBucketLimiter(bucketCapacity, leakRate);

            //The first call has no wait since there were no calls before. It does not increment the queue size.
            var wait = localLeakyBucketLimiter.Limit(out bool limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.Equal(0, wait);

            //The second call increments the queue size by 1. 
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate);
        }

        [Fact]
        public void Limit_Rate_500_Three_Times_Success_Test()
        {
            int leakRate = 500;
            var localLeakyBucketLimiter = new LocalLeakyBucketLimiter(bucketCapacity, leakRate);

            var wait = localLeakyBucketLimiter.Limit(out bool limitExhausted);

            //The first call has no wait since there were no calls before. It does not increment the queue size.
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.Equal(0, wait);

            //The second call increments the queue size by 1. 
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate);

            // The third call increments the queue size by 1.
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate * 2);
        }

        [Fact]
        public void Limit_Rate_500_With_Sleep_Success_Test()
        {
            int leakRate = 500;
            var localLeakyBucketLimiter = new LocalLeakyBucketLimiter(bucketCapacity, leakRate);

            var wait = localLeakyBucketLimiter.Limit(out bool limitExhausted);

            //The first call has no wait since there were no calls before. It does not increment the queue size.
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.Equal(0, wait);

            //The second call increments the queue size by 1. 
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate);

            // The third call increments the queue size by 1.
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate * 2);

            //The third call overflows the bucket capacity.
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate * 3);

            // Move the Clock 1 position forward.
            Thread.Sleep(wait);

            // Retry the last call. This time it should succeed.
            wait = localLeakyBucketLimiter.Limit(out limitExhausted);
            Assert.Equal(IsLimitExhausted(wait, bucketCapacity, leakRate), limitExhausted);
            Assert.True(wait <= leakRate);
        }

        private static bool IsLimitExhausted(int wait, int bucketCapacity, int leakRate)
        {
            return wait >= bucketCapacity * leakRate;
        }
    }
}