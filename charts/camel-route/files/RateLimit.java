import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

import redis.clients.jedis.JedisPooled;

/**
 * Rate-limit "plugin" for Camel K — the ONLY Java file you deploy.
 *
 * It defines no routes. Its single job is to register a bean named "rateLimit"
 * that YAML routes call to enforce a per-key distributed SLA via a Redis token
 * bucket (atomic Lua, Redis server clock, shared across every pod/replica).
 *
 * ── How YAML routes use it ────────────────────────────────────────────────
 *   HTTP  (reject over-limit with 429 + Retry-After, stops the route):
 *     - bean:
 *         ref: rateLimit
 *         method: "http(${exchange}, '<bucket>', <rate/s>, <burst>)"
 *     (${exchange} is required — Camel binds an explicit arg list positionally
 *      and will NOT auto-inject the Exchange.)
 *
 *   Kafka (block until a token is free — backpressure, nothing dropped):
 *     - bean:
 *         ref: rateLimit
 *         method: "block('<bucket>', <rate/s>, <burst>)"
 *
 *   <bucket>  any string; two routes sharing a bucket share one limit.
 *   <rate/s>  sustained tokens per second (double, e.g. 5.0).
 *   <burst>   bucket capacity — how many can arrive back-to-back (int).
 *
 * ── Deploy (plugin + every route in the folder) ───────────────────────────
 *   kamel run rate-limit/plugin/RateLimit.java rate-limit/routes/*.yaml \
 *     --name multi-route-bridge -n camel-k \
 *     --config secret:kafka-scram-credentials \
 *     -d mvn:redis.clients:jedis:5.2.0 -d camel:http \
 *     --resource secret:ratelimit-tls@/etc/tls \
 *     -p quarkus.http.ssl-port=8443 \
 *     -p quarkus.http.ssl.certificate.files=/etc/tls/tls.crt \
 *     -p quarkus.http.ssl.certificate.key-files=/etc/tls/tls.key \
 *     -t prometheus.enabled=true -t prometheus.pod-monitor=false
 *
 * Add a new rate-limited route by dropping another YAML in rate-limit/routes/
 * — no Java change needed.
 */
public class RateLimit extends RouteBuilder {

    @Override
    public void configure() {
        // No routes here — just publish the bean the YAML routes reference by ref="rateLimit".
        bindToRegistry("rateLimit", new Limiter(
            System.getenv().getOrDefault("REDIS_HOST", "redis.redis.svc.cluster.local"),
            Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"))));
    }

    /** The bean invoked from YAML. Thread-safe (JedisPooled is), single shared instance. */
    public static class Limiter {

        // KEYS[1] bucket key; ARGV: rate (tokens/s), capacity, requested.
        // Refills continuously from the Redis server clock, atomically takes tokens when free.
        private static final String LUA = """
            local key = KEYS[1]
            local rate = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local t = redis.call('TIME')
            local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
            local data = redis.call('HMGET', key, 'tokens', 'ts')
            local tokens = tonumber(data[1])
            local ts = tonumber(data[2])
            if tokens == nil or ts == nil then
              tokens = capacity
              ts = now
            end
            local elapsed = now - ts
            if elapsed < 0 then elapsed = 0 end
            tokens = tokens + elapsed * rate
            if tokens > capacity then tokens = capacity end
            local allowed = 0
            if tokens >= requested then
              tokens = tokens - requested
              allowed = 1
            end
            redis.call('HSET', key, 'tokens', tokens, 'ts', now)
            redis.call('EXPIRE', key, math.max(60, math.ceil(capacity / rate) * 2))
            return allowed
            """;

        private final JedisPooled jedis;

        public Limiter(String host, int port) {
            this.jedis = new JedisPooled(host, port);
        }

        /**
         * HTTP style. If the caller is over its SLA, sets 429 + Retry-After and stops the
         * route so nothing downstream runs. YAML must pass the Exchange explicitly, e.g.
         * method="http(${exchange}, 'hh', 5.0, 10)".
         */
        public void http(Exchange e, String key, double ratePerSecond, int burstCapacity) {
            if (!tryAcquire(key, ratePerSecond, burstCapacity)) {
                e.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 429);
                e.getMessage().setHeader("Retry-After", "1");
                e.getMessage().setBody("rate limit exceeded: " + key + "\n");
                e.setRouteStop(true);
            }
        }

        /** Kafka style. Blocks the consumer thread until a token is available (backpressure). */
        public void block(String key, double ratePerSecond, int burstCapacity) {
            while (!tryAcquire(key, ratePerSecond, burstCapacity)) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted while waiting for rate-limit token", ie);
                }
            }
        }

        private boolean tryAcquire(String key, double ratePerSecond, int burstCapacity) {
            Object result = jedis.eval(LUA,
                List.of("ratelimit:" + key),
                List.of(Double.toString(ratePerSecond), Integer.toString(burstCapacity), "1"));
            return ((Long) result) == 1L;
        }
    }
}
