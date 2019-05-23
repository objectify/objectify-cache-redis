# Objectify Cache for Redis

This is a cache plugin for Objectify that uses Redis. It can work with a standalone Redis instance or with Google's Google Cloud Memorystore for Redis service.

## Maven

```xml
<dependencies>
	<dependency>
		<groupId>com.googlecode.objectify</groupId>
		<artifactId>objectify-cache-redis</artifactId>
		<version>(check maven central for latest)</version>
    </dependency>
</dependencies>
```

## Usage

Initialize your ObjectifyFactory with the RedisMemcacheService:

```java
ObjectifyService.init(
		new ObjectifyFactory(
				new RedisMemcacheService("localhost", 6379)));
```

If you want more configuration options, you can construct the `RedisMemcacheService` with a `JedisPool` instance. See the [Jedis documentation](https://github.com/xetorthio/jedis) for more details.

## Help

Help is provided in the
[Objectify App Engine User Group](https://groups.google.com/forum/?fromgroups#!forum/objectify-appengine)
