package com.leonardonogueira.nosqlmsg.api;

import com.leonardonogueira.nosqlmsg.kafka.KafkaOrderConsumers;
import com.leonardonogueira.nosqlmsg.kafka.KafkaOrderProducer;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kafka")
public class KafkaLabController {

    private final KafkaOrderProducer producer;
    private final KafkaOrderConsumers consumers;

    public KafkaLabController(KafkaOrderProducer producer, KafkaOrderConsumers consumers) {
        this.producer = producer;
        this.consumers = consumers;
    }

    /**
     * TESTE 1: Publica um evento no Kafka.
     * Repare que ao enviar eventos com o MESMO 'orderId', o Kafka calcula o hash
     * da chave e sempre entrega na MESMA PARTIÇÃO (garantindo ordem FIFO!).
     */
    @PostMapping("/publish")
    public CompletableFuture<Map<String, Object>> publishEvent(
            @RequestParam(defaultValue = "PED-9001") String orderId,
            @RequestParam(defaultValue = "CLI-LEO") String customerId,
            @RequestParam(defaultValue = "1500.00") BigDecimal amount,
            @RequestParam(defaultValue = "ORDER_CREATED") String eventType) {
        return producer.publishOrderEvent(orderId, customerId, amount, eventType);
    }

    /**
     * TESTE 2: Consulta o histórico de consumo dos DOIS Consumer Groups.
     * Você verá que tanto o 'audit-consumer-group' quanto o 'notification-consumer-group'
     * processaram 100% dos eventos (Fan-out nativo do Kafka)!
     */
    @GetMapping("/consumed-history")
    public List<Map<String, Object>> getConsumedHistory() {
        return consumers.getProcessedEvents();
    }
}

