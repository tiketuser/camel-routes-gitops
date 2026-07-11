import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.BindToRegistry;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.jsse.KeyManagersParameters;
import org.apache.camel.support.jsse.KeyStoreParameters;
import org.apache.camel.support.jsse.SSLContextParameters;
import org.apache.camel.support.jsse.TrustManagersParameters;

import redis.clients.jedis.JedisPooled;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import jakarta.jms.JMSException;

/**
 * Rate-limit "plugin" for Camel K — the ONLY Java file you deploy.
 *
 * It defines no routes. Its single job is to register a bean named "rateLimit"
 * that YAML routes call to enforce a per-key distributed SLA via a Redis token
 * bucket (atomic Lua, Redis server clock, shared across every pod/replica).
 * It also registers "echoClientSSL", the mTLS context the https-to-https route
 * uses to call echo-server: a client keystore (bridge's identity, presented to
 * echo-server) and a truststore (echo-server's cert, so the bridge only talks
 * to that server) — both loaded from the echo-client-tls secret.
 * It also registers "mqConnectionFactory", the IBM MQ JMS connection factory the
 * mq-to-mq / https-to-mq / mq-to-https routes use via `jms:queue:...`.
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
 *     --config secret:mq-app-credentials \
 *     -d mvn:redis.clients:jedis:5.2.0 -d camel:http \
 *     -d camel:jms -d mvn:com.ibm.mq:com.ibm.mq.jakarta.client:9.4.4.0 \
 *     --resource secret:ratelimit-tls@/etc/tls \
 *     --resource secret:echo-client-tls@/etc/echo-tls \
 *     --resource secret:bridge-client-ca@/etc/tls-ca \
 *     -p quarkus.http.ssl-port=8443 \
 *     -p quarkus.http.ssl.certificate.files=/etc/tls/tls.crt \
 *     -p quarkus.http.ssl.certificate.key-files=/etc/tls/tls.key \
 *     -p quarkus.http.ssl.client-auth=REQUIRED \
 *     -p quarkus.http.ssl.certificate.trust-store-file=/etc/tls-ca/client-ca.crt \
 *     -p quarkus.http.ssl.certificate.trust-store-file-type=PEM \
 *     -p quarkus.http.insecure-requests=disabled \
 *     -t prometheus.enabled=true -t prometheus.pod-monitor=false
 *
 * See rate-limit/README.md for the full mTLS secret-creation steps (ratelimit-tls,
 * bridge-server-ca, echo-server-tls, echo-client-tls, caller-client-tls, bridge-client-ca)
 * this command assumes already exist.
 *
 * Add a new rate-limited route by dropping another YAML in rate-limit/routes/
 * — no Java change needed.
 */
public class RateLimit extends RouteBuilder {

    // Matches the -passout/-passin password used when the echo-client-keystore.p12 /
    // echo-server-truststore.p12 files were generated (openssl pkcs12 -export).
    private static final String ECHO_TLS_PASSWORD =
        System.getenv().getOrDefault("ECHO_TLS_PASSWORD", "changeit");

    @Override
    public void configure() {
        // No routes here — just publish the beans the YAML routes reference by ref=.
        bindToRegistry("rateLimit", new Limiter(
            System.getenv().getOrDefault("REDIS_HOST", "redis.redis.svc.cluster.local"),
            Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"))));
    }

    /** mTLS context for the bridge->echo-server hop: client cert + server truststore. */
    @BindToRegistry("echoClientSSL")
    public SSLContextParameters echoClientSSL() {
        KeyStoreParameters keyStore = new KeyStoreParameters();
        keyStore.setResource("file:/etc/echo-tls/keystore.p12");
        keyStore.setPassword(ECHO_TLS_PASSWORD);
        KeyManagersParameters keyManagers = new KeyManagersParameters();
        keyManagers.setKeyStore(keyStore);
        keyManagers.setKeyPassword(ECHO_TLS_PASSWORD);

        KeyStoreParameters trustStore = new KeyStoreParameters();
        trustStore.setResource("file:/etc/echo-tls/truststore.p12");
        trustStore.setPassword(ECHO_TLS_PASSWORD);
        TrustManagersParameters trustManagers = new TrustManagersParameters();
        trustManagers.setKeyStore(trustStore);

        SSLContextParameters sslContextParameters = new SSLContextParameters();
        sslContextParameters.setKeyManagers(keyManagers);
        sslContextParameters.setTrustManagers(trustManagers);
        return sslContextParameters;
    }

    /**
     * IBM MQ connection factory for the mq-to-mq / https-to-mq / mq-to-https routes.
     * Username/password aren't set here — routes pass them per-endpoint via
     * `jms:queue:...?username={{mq.user}}&password={{mq.password}}`, populated from
     * the mq-app-credentials secret (same pattern as {{kafka.user}}/{{kafka.password}}).
     */
    @BindToRegistry("mqConnectionFactory")
    public MQConnectionFactory mqConnectionFactory() throws JMSException {
        MQConnectionFactory connectionFactory = new MQConnectionFactory();
        connectionFactory.setHostName(System.getenv().getOrDefault("MQ_HOST", "mq.mq.svc.cluster.local"));
        connectionFactory.setPort(Integer.parseInt(System.getenv().getOrDefault("MQ_PORT", "1414")));
        connectionFactory.setChannel(System.getenv().getOrDefault("MQ_CHANNEL", "DEV.APP.SVRCONN"));
        connectionFactory.setQueueManager(System.getenv().getOrDefault("MQ_QMGR", "QM1"));
        // 1 == com.ibm.msg.client.wmq.WMQConstants.WMQ_CM_CLIENT (client/TCP transport,
        // as opposed to 0 == bindings mode). Hardcoded because that companion class
        // doesn't resolve on the joor runtime-compile classpath even though
        // MQConnectionFactory (same jar) does; the constant is stable across MQ versions.
        connectionFactory.setTransportType(1);
        return connectionFactory;
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
