package com.leonardonogueira.nosqlmsg.api;

import com.leonardonogueira.nosqlmsg.redis.ProductCacheService;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/redis")
public class RedisLabController {

    private final ProductCacheService cacheService;

    public RedisLabController(ProductCacheService cacheService) {
        this.cacheService = cacheService;
    }

    /**
     * TESTE 1: Cache-Aside Pattern
     * 1ª chamada: CACHE MISS (~120ms - busca no banco e grava no Redis)
     * 2ª chamada em diante: CACHE HIT (< 5ms - busca direto na memória RAM do Redis)
     */
    @GetMapping("/products/{id}")
    public Map<String, Object> getProduct(@PathVariable Long id) {
        return cacheService.findProductWithCacheAside(id);
    }

    /**
     * TESTE 2: Invalidação de Cache (Cache Eviction)
     * Atualiza o preço no banco e apaga a chave do Redis.
     * O próximo GET voltará a ser CACHE MISS para trazer o valor novo!
     */
    @PutMapping("/products/{id}/price")
    public Map<String, Object> updatePrice(
            @PathVariable Long id,
            @RequestParam BigDecimal newPrice) {
        return cacheService.updateProductPrice(id, newPrice);
    }

    /**
     * TESTE 3: Concorrência com Distributed Lock (Redis SET NX PX)
     */
    @PostMapping("/flash-sale/buy")
    public Map<String, Object> buyFlashSale(
            @RequestParam(defaultValue = "1") Long productId,
            @RequestParam(defaultValue = "Cliente-A") String customerId) {
        return cacheService.purchaseFlashSaleItemWithLock(productId, customerId);
    }
}

