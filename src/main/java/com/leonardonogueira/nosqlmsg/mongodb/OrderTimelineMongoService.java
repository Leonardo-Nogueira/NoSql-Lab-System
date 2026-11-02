package com.leonardonogueira.nosqlmsg.mongodb;

import com.leonardonogueira.nosqlmsg.mongodb.OrderTimelineDocument.ShippingAddress;
import com.leonardonogueira.nosqlmsg.mongodb.OrderTimelineDocument.TimelineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
public class OrderTimelineMongoService {

    private static final Logger log = LoggerFactory.getLogger(OrderTimelineMongoService.class);

    private final OrderTimelineRepository repository;
    private final MongoTemplate mongoTemplate; // Usado para operacoes atomicas de baixo nivel ($push, $set, $inc)

    public OrderTimelineMongoService(OrderTimelineRepository repository, MongoTemplate mongoTemplate) {
        this.repository = repository;
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * 1. CRIAÇÃO DO DOCUMENTO BSON (EMBEDDING):
     * Salva o agregado inteiro com endereço e primeiro evento de timeline em 1 único documento!
     */
    public OrderTimelineDocument createOrder(
            String orderId,
            String customerId,
            BigDecimal amount,
            String street,
            String city) {

        ShippingAddress address = new ShippingAddress(street, "100", city, "SP", "01310-100");
        OrderTimelineDocument doc = new OrderTimelineDocument(orderId, customerId, amount, address);

        OrderTimelineDocument saved = repository.save(doc);
        log.info("🍃 [MONGODB SAVE] Pedido NoSQL gravado com sucesso! ObjectId='{}', OrderId='{}'",
                saved.getId(), saved.getOrderId());
        return saved;
    }

    /**
     * 2. ATUALIZAÇÃO ATÔMICA COM $push e $set (PADRÃO SENIOR DE PERFORMANCE):
     * Em vez de ler o documento inteiro na memória do Java, adicionamos o evento
     * diretamente no array do disco do MongoDB em 1 única operação atômica de rede!
     */
    public Map<String, Object> appendTimelineEventAtomically(String orderId, String newStatus, String description) {
        long start = System.currentTimeMillis();

        Query query = Query.query(Criteria.where("orderId").is(orderId));

        TimelineEvent newEvent = new TimelineEvent(newStatus, description, Instant.now());

        // Operador $push adiciona ao array, $set atualiza status e data
        Update update = new Update()
                .push("timeline", newEvent)
                .set("currentStatus", newStatus)
                .set("updatedAt", Instant.now());

        var updateResult = mongoTemplate.updateFirst(query, update, OrderTimelineDocument.class);

        long elapsed = System.currentTimeMillis() - start;

        if (updateResult.getMatchedCount() == 0) {
            throw new RuntimeException("Pedido nao encontrado no MongoDB com orderId: " + orderId);
        }

        log.info("⚡ [MONGODB ATOMIC $push] Evento '{}' inserido atomicamente no pedido {} em {}ms!",
                newStatus, orderId, elapsed);

        return Map.of(
                "orderId", orderId,
                "operation", "ATOMIC_$push_and_$set",
                "matchedCount", updateResult.getMatchedCount(),
                "modifiedCount", updateResult.getModifiedCount(),
                "elapsedTimeMs", elapsed,
                "addedEvent", newEvent
        );
    }

    public OrderTimelineDocument getByOrderId(String orderId) {
        return repository.findByOrderId(orderId)
                .orElseThrow(() -> new RuntimeException("Pedido " + orderId + " nao existe no MongoDB"));
    }

    public List<OrderTimelineDocument> listByCustomer(String customerId) {
        return repository.findByCustomerIdOrderByCreatedAtDesc(customerId);
    }
}

