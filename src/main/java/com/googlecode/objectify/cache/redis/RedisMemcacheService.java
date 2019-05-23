package com.googlecode.objectify.cache.redis;

import com.googlecode.objectify.cache.IdentifiableValue;
import com.googlecode.objectify.cache.MemcacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * We store everything using java serialization. In theory we could be more efficient with some sort of custom
 * translator, but we really just store Entity objects, which are pretty complex, so let's just go with serialization.
 *
 * Storage format: We prefix all values with a 16-byte UUID version number, which is randomized on every put.
 */
@RequiredArgsConstructor
@Slf4j
public class RedisMemcacheService implements MemcacheService {
	/**
	 * If we give Jedis bytes, we get back bytes
	 */
	private static final byte[] OK_BYTES = "OK".getBytes(StandardCharsets.UTF_8);

	/** */
	private final JedisPool jedisPool;

	public RedisMemcacheService(final String host) {
		this(new JedisPool(host));
	}

	public RedisMemcacheService(final String host, final int port) {
		this(new JedisPool(host, port));
	}

	/** */
	private RedisIdentifiableValue fromCacheValue(final byte[] thing) {
		if (thing == null)
			return null;

		if (thing.length == 0)
			return null;

		try {
			return RedisIdentifiableValue.fromRedisString(thing);
		} catch (final Exception e) {
			log.error("Error deserializing from redis", e);
			return null;
		}
	}

	/** */
	private RedisIdentifiableValue getValue(final String key) {
		final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);

		try (final Jedis jedis = jedisPool.getResource()) {
			final byte[] bytes = jedis.get(binKey);
			return fromCacheValue(bytes);
		}
	}

	/** */
	private void putValue(final String key, final RedisIdentifiableValue value) {
		final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);

		try (final Jedis jedis = jedisPool.getResource()) {
			jedis.set(binKey, value.toRedisString());
		}
	}

	@Override
	public Object get(final String key) {
		final RedisIdentifiableValue iv = getValue(key);
		return iv == null ? null : iv.getValue();
	}

	@Override
	public Map<String, IdentifiableValue> getIdentifiables(final Collection<String> keys) {
		// Can't use streams because they don't allow nulls
		//return keys.stream().collect(Collectors.toMap(Functions.identity(), this::getIdentifiable));
		final Map<String, IdentifiableValue> result = new LinkedHashMap<>();

		for (final String key : keys) {
			final IdentifiableValue iv = getIdentifiable(key);
			result.put(key, iv);
		}

		return result;
	}

	private IdentifiableValue getIdentifiable(final String key) {
		final RedisIdentifiableValue foundIv = getValue(key);

		if (foundIv != null)
			return foundIv;

		// Force a null value into the system
		final RedisIdentifiableValue nullIv = new RedisIdentifiableValue(null);

		putValue(key, nullIv);

		return nullIv;
	}

	@Override
	public Map<String, Object> getAll(final Collection<String> keys) {
		final byte[][] binKeys = keys.stream().map(key -> key.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);

		final List<byte[]> fetched;
		try (final Jedis jedis = jedisPool.getResource()) {
			fetched = jedis.mget(binKeys);
		}

		final Map<String, Object> result = new LinkedHashMap<>();

		final Iterator<String> keysIt = keys.iterator();
		int index = 0;
		while (keysIt.hasNext()) {
			final String key = keysIt.next();
			final byte[] value = fetched.get(index);

			final RedisIdentifiableValue iv = RedisIdentifiableValue.fromRedisString(value);

			result.put(key, iv.getValue());

			index++;
		}

		return result;
	}

	@Override
	public void put(final String key, final Object value) {
		putValue(key, new RedisIdentifiableValue(value));
	}

	@Override
	public Set<String> putIfUntouched(final Map<String, CasPut> values) {
		final Set<String> successes = new HashSet<>();

		values.forEach((key, cput) -> {
			final RedisIdentifiableValue iv = (RedisIdentifiableValue)cput.getIv();
			final byte[] lastVersion = iv.getVersionRedisString();

			final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);
			final RedisIdentifiableValue nextIv = new RedisIdentifiableValue(cput.getNextToStore());

			try (final Jedis jedis = jedisPool.getResource()) {
				final byte[] response = executePutScript(jedis, binKey, lastVersion, nextIv.toRedisString(), cput.getExpirationSeconds());
				if (Arrays.equals(OK_BYTES, response)) {
					successes.add(key);
				}
			}
		});

		return successes;
	}

	private static final byte[] PUT_SCRIPT_WITH_EXPIRATION = "local value = redis.call('get', KEYS[1]); if (value:sub(1, 16) == KEYS[2]) then return redis.call('set', KEYS[1], KEYS[3], 'EX', KEYS[4]) end".getBytes(StandardCharsets.UTF_8);

	private static final byte[] PUT_SCRIPT_WITHOUT_EXPIRATION = "local value = redis.call('get', KEYS[1]); if (value:sub(1, 16) == KEYS[2]) then return redis.call('set', KEYS[1], KEYS[3]) end".getBytes(StandardCharsets.UTF_8);

	/**
	 * @param expiration if 0 or less, will not include an expiration
	 */
	private byte[] executePutScript(final Jedis jedis, final byte[] binKey, final byte[] lastVersion, final byte[] value, final int expiration) {
		if (expiration > 0) {
			// Redis expects the number as a string
			final byte[] exp = Integer.toString(expiration).getBytes(StandardCharsets.UTF_8);
			return (byte[])jedis.eval(PUT_SCRIPT_WITH_EXPIRATION, 4, binKey, lastVersion, value, exp);
		} else {
			return (byte[])jedis.eval(PUT_SCRIPT_WITHOUT_EXPIRATION, 3, binKey, lastVersion, value);
		}
	}

	@Override
	public void putAll(final Map<String, Object> values) {
		values.forEach(this::put);
	}

	@Override
	public void deleteAll(final Collection<String> keys) {
		keys.forEach(this::delete);
	}

	private void delete(final String key) {
		final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);

		try (final Jedis jedis = jedisPool.getResource()) {
			jedis.del(binKey);
		}
	}
}
