package com.googlecode.objectify.cache.redis;

import com.googlecode.objectify.cache.IdentifiableValue;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

/**
 * Supports the MemcacheService contract and also provides the redis strings, which must contain the version UUID.
 */
@Data
@RequiredArgsConstructor
public class RedisIdentifiableValue implements IdentifiableValue {
	private final UUID version;

	@Getter
	private final Object value;

	public RedisIdentifiableValue(final Object value) {
		this(UUID.randomUUID(), value);
	}

	/** */
	@SneakyThrows
	public byte[] toRedisString() {
		final ByteArrayOutputStream out = new ByteArrayOutputStream();

		final DataOutputStream dataOut = new DataOutputStream(out);
		dataOut.writeLong(version.getMostSignificantBits());
		dataOut.writeLong(version.getLeastSignificantBits());

		final ObjectOutputStream objectOut = new ObjectOutputStream(out);
		objectOut.writeObject(value);

		return out.toByteArray();
	}

	/** Just the redis string version of the version, not the body */
	@SneakyThrows
	public byte[] getVersionRedisString() {
		final ByteArrayOutputStream out = new ByteArrayOutputStream();

		final DataOutputStream dataOut = new DataOutputStream(out);
		dataOut.writeLong(version.getMostSignificantBits());
		dataOut.writeLong(version.getLeastSignificantBits());

		return out.toByteArray();
	}

	/** */
	@SneakyThrows
	public static RedisIdentifiableValue fromRedisString(final byte[] input) {
		final ByteArrayInputStream in = new ByteArrayInputStream(input);

		final DataInputStream dataIn = new DataInputStream(in);
		final long mostSignificant = dataIn.readLong();
		final long leastSignificant = dataIn.readLong();
		final UUID stamp = new UUID(mostSignificant, leastSignificant);

		final ObjectInputStream objectIn = new ObjectInputStream(in);
		final Object value = objectIn.readObject();

		return new RedisIdentifiableValue(stamp, value);
	}
}
