package rollingratelimiter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class RateLimiter {

	private String namespace;
	private JedisPool jedisPool;
	private long interval;
	private long maxInInterval;
	private Long minDifference;
	private boolean storeBlocked;

	private RateLimiter() {
	}

	public String getNamespace() {
		return namespace;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public long getInterval() {
		return interval;
	}

	public long getMaxInInterval() {
		return maxInInterval;
	}

	public Long getMinDifference() {
		return minDifference;
	}

	public boolean isStoreBlocked() {
		return storeBlocked;
	}

	public static RateLimiterBuilder builder() {
		return new RateLimiterBuilder();
	}

	public static class RateLimiterBuilder {

		private String namespace = "rate-limiter-";
		private JedisPool jedisPool;
		private long interval;
		private long maxInInterval;
		private Long minDifference;
		private boolean storeBlocked = true;

		private RateLimiterBuilder() {
		}

		public RateLimiterBuilder namespace(String namespace) {
			this.namespace = namespace;
			return this;
		}

		public RateLimiterBuilder jedisPool(JedisPool jedisPool) {
			this.jedisPool = jedisPool;
			return this;
		}

		public RateLimiterBuilder interval(long interval) {
			this.interval = interval;
			return this;
		}

		public RateLimiterBuilder maxInInterval(long maxInInterval) {
			this.maxInInterval = maxInInterval;
			return this;
		}

		public RateLimiterBuilder minDifference(Long minDifference) {
			this.minDifference = minDifference;
			return this;
		}

		public RateLimiterBuilder storeBlocked(boolean storeBlocked) {
			this.storeBlocked = storeBlocked;
			return this;
		}

		public RateLimiter build() {
			RateLimiter rateLimiter = new RateLimiter();
			rateLimiter.namespace = namespace;
			rateLimiter.jedisPool = jedisPool;
			rateLimiter.interval = interval;
			rateLimiter.maxInInterval = maxInInterval;
			rateLimiter.minDifference = minDifference;
			rateLimiter.storeBlocked = storeBlocked;
			return rateLimiter;
		}
	}

	public long check(String id) {
		long intervalMicros = interval * 1000;

		long now = System.currentTimeMillis() * 1000;
		String key = namespace + id;
		long clearBefore = now - intervalMicros;

		try (Jedis jedis = jedisPool.getResource()) {
			Transaction trans = jedis.multi();
			trans.zremrangeByScore(key, 0, clearBefore);
			trans.zrange(key, 0, -1);
			if (storeBlocked) {
				trans.zadd(key, now, String.valueOf(now));
				trans.expire(key, (int) Math.ceil(intervalMicros / 1_000_000));
			}
			List<Object> transResults = trans.exec();
			LinkedHashSet<String> zRangeResults = (LinkedHashSet<String>) transResults.get(1);
			List<Long> userSet = zRangeResults.stream().map(Long::parseLong).collect(Collectors.toList());

			boolean tooManyInInterval = userSet.size() >= maxInInterval;
			Long timeUntilNextIntervalOpportunity = null;
			if (userSet.size() > 0) {
				timeUntilNextIntervalOpportunity = userSet.get(0) + intervalMicros - now;
			}
			Long timeSinceLastRequest = null;
			if (minDifference != null && userSet.size() > 0) {
				timeSinceLastRequest = now - userSet.get(userSet.size() - 1);
			}

			Long timeLeft = null;
			if (tooManyInInterval) {
				timeLeft = (timeUntilNextIntervalOpportunity == null) ? null : timeUntilNextIntervalOpportunity / 1000;
			} else if (timeSinceLastRequest != null && minDifference != null && timeSinceLastRequest < minDifference * 1000) {
				Long timeUntilNextMinDifferenceOpportunity = minDifference - timeSinceLastRequest;
				if (timeUntilNextIntervalOpportunity == null) {
					timeLeft = timeUntilNextMinDifferenceOpportunity;
				} else {
					timeLeft = Math.min(timeUntilNextIntervalOpportunity, timeUntilNextMinDifferenceOpportunity);
				}
				timeLeft = (long) Math.floor(timeLeft / 1000); // convert from microseconds for user readability.
			}

			long result;
			if (timeLeft == null) {
				result = 0;
				if (!storeBlocked) {
					trans = jedis.multi();
					trans.zadd(key, now, String.valueOf(now));
					trans.expire(key, (int) Math.ceil(intervalMicros / 1_000_000));
					trans.exec();
				}
			} else {
				result = timeLeft;
			}
			return result;
		}
	}
}
