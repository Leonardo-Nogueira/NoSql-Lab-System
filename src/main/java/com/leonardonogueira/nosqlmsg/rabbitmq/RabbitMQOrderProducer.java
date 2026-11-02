package com.leonardonogueira.nosqlmsg.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

@Service
public class RabbitMQOrderProducer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQOrderProducer.class);

    private final RabbitTemplate rabbitTemplate;

    public RabbitMQOrderProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public record PaymentMessage(
            String orderId,
            BigDecimal amount,
            boolean simulateError,
            Instant timestamp
    ) {}

    /**
     * Publica na DIRECT EXCHANGE usando routing key exata ('order.payment.process'):
     */
    public Map<String, Object> sendPaymentMessage(String orderId, BigDecimal amount, boolean simulateError) {
        PaymentMessage message = new PaymentMessage(orderId, amount, simulateError, Instant.now());

        log.info("📤 [RABBITMQ PRODUCER] Enviando para Exchange='{}' com RoutingKey='{}' (simulateError={})",
                RabbitMQConfig.DIRECT_EXCHANGE, RabbitMQConfig.PAYMENT_ROUTING_KEY, simulateError);

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.DIRECT_EXCHANGE,
                RabbitMQConfig.PAYMENT_ROUTING_KEY,
                message
        );

        return Map.of(
                "status", "ENQUEUED_IN_RABBITMQ",
                "exchange", RabbitMQConfig.DIRECT_EXCHANGE,
                "routingKey", RabbitMQConfig.PAYMENT_ROUTING_KEY,
                "targetQueue", RabbitMQConfig.PAYMENT_QUEUE,
                "message", message
        );
    }

    /**
     * Publica na TOPIC EXCHANGE usando chaves flexíveis com curingas (* e #):
     */
    public Map<String, Object> sendTopicMessage(String routingKey, String content) {
        Map<String, Object> payload = Map.of(
                "content", content,
                "routingKeyUsed", routingKey,
                "publishedAt", Instant.now().toString()
        );

        log.info("📤 [RABBITMQ TOPIC PRODUCER] Enviando para Exchange='{}' com RoutingKey='{}'",
                RabbitMQConfig.TOPIC_EXCHANGE, routingKey);

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.TOPIC_EXCHANGE,
                routingKey,
                payload
        );

        return Map.of(
                "status", "ROUTED_VIA_TOPIC_EXCHANGE",
                "exchange", RabbitMQConfig.TOPIC_EXCHANGE,
                "routingKey", routingKey,
                "payload", payload
        );
    }
}

