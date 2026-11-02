package com.leonardonogueira.nosqlmsg.redis;

import com.leonardonogueira.nosqlmsg.domain.Product;
import com.leonardonogueira.nosqlmsg.repository.ProductRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

@Service
public class ProductCacheService {

    private static final Logger log = LoggerFactory.getLogger(ProductCacheService.class);
    private static final String CACHE_PREFIX = "product:";

    private final ProductRepository productRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisDistributedLock distributedLock;

    public ProductCacheService(
            ProductRepository productRepository,
            RedisTemplate<String, Object> redisTemplate,
            RedisDistributedLock distributedLock) {
        this.productRepository = productRepository;
        this.redisTemplate = redisTemplate;
        this.distributedLock = distributedLock;
    }

    // =========================================================================
    // 1. PADRAO CACHE-ASIDE (LAZY LOADING) COM TTL
    // =========================================================================
    public Map<String, Object> findProductWithCacheAside(Long id) {
        String cacheKey = CACHE_PREFIX + id;
        long start = System.currentTimeMillis();

        // 1. Tenta buscar no REDIS primeiro:
        Object cached = redisTemplate.opsForValue().get(cacheKey);

        if (cached != null) {
            long elapsed = System.currentTimeMillis() - start;
            log.info("⚡ [CACHE HIT] Produto {} encontrado no REDIS em {}ms!", id, elapsed);
            return Map.of(
                    "strategy", "CACHE_HIT (Memoria RAM do Redis)",
                    "elapsedTimeMs", elapsed,
                    "product", cached
            );
        }

        // 2. [CACHE MISS] Nao esta no Redis -> Vai ao Banco de Dados Relacional:
        log.warn("🔍 [CACHE MISS] Produto {} nao esta no Redis! Consultando Banco de Dados...", id);
        simulateDatabaseLatency(120); // Simula custo de I/O de disco / query pesada

        Product productFromDb = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Produto nao encontrado com id: " + id));

        // 3. Salva no REDIS com TTL de 45 segundos (evita cache infinito / stale data):
        redisTemplate.opsForValue().set(cacheKey, productFromDb, Duration.ofSeconds(45));
        long elapsed = System.currentTimeMillis() - start;

        log.info("💾 [CACHE POPULATED] Produto {} salvo no Redis com TTL de 45s. Tempo total: {}ms", id, elapsed);

        return Map.of(
                "strategy", "CACHE_MISS (Lido do Banco e Salvo no Redis)",
                "elapsedTimeMs", elapsed,
                "product", productFromDb
        );
    }

    // =========================================================================
    // 2. INVALIDACAO DE CACHE NA ATUALIZACAO (Cache Eviction)
    // =========================================================================
    public Map<String, Object> updateProductPrice(Long id, BigDecimal newPrice) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Produto nao encontrado"));

        BigDecimal oldPrice = product.getPrice();
        product.setPrice(newPrice);
        productRepository.save(product);

        // REGRA DE OURO: Invalida (deleta) a chave no Redis!
        String cacheKey = CACHE_PREFIX + id;
        Boolean deleted = redisTemplate.delete(cacheKey);

        log.info("🗑️ [CACHE INVALIDATED] Chave '{}' deletada do Redis apos update de preco!", cacheKey);

        return Map.of(
                "action", "PRICE_UPDATED",
                "oldPrice", oldPrice,
                "newPrice", newPrice,
                "cacheEvicted", Boolean.TRUE.equals(deleted),
                "message", "O cache antigo foi removido! A proxima leitura fara Cache-Aside atualizando o dado fresco!"
        );
    }

    // =========================================================================
    // 3. COMPRA CONCORRENTE COM DISTRIBUTED LOCK
    // =========================================================================
    public Map<String, Object> purchaseFlashSaleItemWithLock(Long productId, String customerId) {
        String lockKey = "lock:flash-sale:product:" + productId;
        String lockToken = customerId + "-" + UUID.randomUUID();

        // Tenta adquirir o lock no Redis por 5 segundos:
        boolean lockAcquired = distributedLock.tryAcquire(lockKey, lockToken, Duration.ofSeconds(5));

        if (!lockAcquired) {
            // OUTRO PROCESSO ESTA EXECUTANDO! Rejeita para proteger a consistencia:
            return Map.of(
                    "status", "BLOCKED_BY_DISTRIBUTED_LOCK",
                    "reason", "Concorrencia detectada! Outro pod/transacao esta comprando este item agora. Tente novamente em instantes.",
                    "customerId", customerId
            );
        }

        try {
            log.info("🛒 [FLASH SALE] Cliente '{}' entrou na secao critica protegida pelo Lock!", customerId);
            simulateDatabaseLatency(80); // Simula processamento de pagamento

            Product product = productRepository.findById(productId).orElseThrow();
            if (product.getStockQuantity() <= 0) {
                return Map.of("status", "OUT_OF_STOCK", "message", "Estoque esgotado!");
            }

            product.setStockQuantity(product.getStockQuantity() - 1);
            productRepository.save(product);

            // Invalida o cache para refletir o novo estoque
            redisTemplate.delete(CACHE_PREFIX + productId);

            return Map.of(
                    "status", "PURCHASE_SUCCESS",
                    "customerId", customerId,
                    "remainingStock", product.getStockQuantity()
            );

        } finally {
            // REGRA DE OURO: Sempre liberar o lock no bloco finally via Script Lua!
            distributedLock.release(lockKey, lockToken);
        }
    }

    private void simulateDatabaseLatency(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PostConstruct
    public void seedInitialProducts() {
        if (productRepository.count() == 0) {
            productRepository.save(new Product("PlayStation 5 Slim 1TB", new BigDecimal("3499.00"), 5));
            productRepository.save(new Product("MacBook Pro M3 Max", new BigDecimal("19999.00"), 2));
            productRepository.save(new Product("Monitor Gamer 34 Ultrawide", new BigDecimal("2799.00"), 10));
            log.info("🌱 [SEED] 3 produtos cadastrados no banco relacional.");
        }
    }
}

