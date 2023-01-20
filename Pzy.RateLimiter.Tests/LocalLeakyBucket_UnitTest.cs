using Pzy.RateLimiter.LeakyBucket;
using Xunit;

namespace Pzy.RateLimiter.Tests
{
    public class LocalLeakyBucket_UnitTest
    {
        private const int bucketCapacity = 2;

        [Fact]
        public void Limit_Rate_500_Two_Times_Success_Test()
        {
            int leakRate = 500;
            var localLeakyBucketLimiter = new LocalLeakyBucketLimiter(bucketCapacity, leakRate);

            //The first call has no wait since there were no calls before. It does not increment the queue size.
            var executable = localLeakyBucketLimiter.Acquire(out long waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.Equal(0, waitMilliseconds);

            //The second call increments the queue size by 1. 
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate);
        }

        [Fact]
        public void Limit_Rate_500_Three_Times_Success_Test()
        {
            int leakRate = 500;
            var localLeakyBucketLimiter = new LocalLeakyBucketLimiter(bucketCapacity, leakRate);

            var executable = localLeakyBucketLimiter.Acquire(out long waitMilliseconds);

            //The first call has no wait since there were no calls before. It does not increment the queue size.
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.Equal(0, waitMilliseconds);

            //The second call increments the queue size by 1. 
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate);

            // The third call increments the queue size by 1.
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate * 2);
        }

        [Fact]
        public void Limit_Rate_500_With_Sleep_Success_Test()
        {
            int leakRate = 500;
            var localLeakyBucketLimiter = new LocalLeakyBucketLimiter(bucketCapacity, leakRate);

            var executable = localLeakyBucketLimiter.Acquire(out long waitMilliseconds);

            //The first call has no wait since there were no calls before. It does not increment the queue size.
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.Equal(0, waitMilliseconds);

            //The second call increments the queue size by 1. 
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate);

            // The third call increments the queue size by 1.
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate * 2);

            //The third call overflows the bucket capacity.
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate * 3);

            // Move the Clock 1 position forward.
            Thread.Sleep((int)waitMilliseconds);

            // Retry the last call. This time it should succeed.
            executable = localLeakyBucketLimiter.Acquire(out waitMilliseconds);
            Assert.Equal(IsExecutable(waitMilliseconds, bucketCapacity, leakRate), executable);
            Assert.True(waitMilliseconds <= leakRate);
        }

        private static bool IsExecutable(long wait, int bucketCapacity, int leakRate)
        {
            return wait < bucketCapacity * leakRate;
        }
    }
}