package com.leonardonogueira.nosqlmsg.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * IMPLEMENTACAO DE LOCK DISTRIBUIDO COM REDIS (SET NX PX + LUA RELEASE):
 * Usado para coordenar operacoes criticas concorrentes entre multiplos pods/instancias.
 */
@Component
public class RedisDistributedLock {

    private static final Logger log = LoggerFactory.getLogger(RedisDistributedLock.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisScript<Long> releaseLockScript;

    public RedisDistributedLock(RedisTemplate<String, Object> redisTemplate, RedisScript<Long> releaseLockScript) {
        this.redisTemplate = redisTemplate;
        this.releaseLockScript = releaseLockScript;
    }

    /**
     * Tenta adquirir o Lock no Redis:
     * - SET key token NX PX <ttlMs>
     * @return true se obteve o lock, false se outro pod ja esta executando!
     */
    public boolean tryAcquire(String lockKey, String lockToken, Duration ttl) {
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, lockToken, ttl);
        boolean success = Boolean.TRUE.equals(acquired);

        if (success) {
            log.info("🔒 [REDIS LOCK] Lock ADQUIRIDO com sucesso! Chave='{}', Token='{}', TTL={}s",
                    lockKey, lockToken, ttl.toSeconds());
        } else {
            log.warn("🛑 [REDIS LOCK] Conflito concorrente! Chave='{}' ja esta BLOQUEADA por outro processo!", lockKey);
        }

        return success;
    }

    /**
     * Libera o Lock de forma atomica usando o Script Lua:
     * So apaga a chave se ela ainda contiver o mesmo lockToken!
     */
    public boolean release(String lockKey, String lockToken) {
        Long result = redisTemplate.execute(releaseLockScript, List.of(lockKey), lockToken);
        boolean released = result != null && result > 0;

        if (released) {
            log.info("🔓 [REDIS LOCK] Lock LIBERADO com sucesso! Chave='{}'", lockKey);
        } else {
            log.warn("⚠️ [REDIS LOCK] Tentativa de liberar chave expirada ou de outro pod! Chave='{}'", lockKey);
        }

        return released;
    }
}

