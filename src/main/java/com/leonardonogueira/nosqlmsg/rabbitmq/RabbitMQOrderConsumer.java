package com.leonardonogueira.nosqlmsg.rabbitmq;

import com.leonardonogueira.nosqlmsg.rabbitmq.RabbitMQOrderProducer.PaymentMessage;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class RabbitMQOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQOrderConsumer.class);

    private final List<Map<String, Object>> dlqAuditHistory = new CopyOnWriteArrayList<>();
    private final List<Map<String, Object>> topicReceiptHistory = new CopyOnWriteArrayList<>();

    // =========================================================================
    // 1. CONSUMO COM MANUAL ACK vs MANUAL NACK (Fila de Pagamento)
    // =========================================================================
    @RabbitListener(queues = RabbitMQConfig.PAYMENT_QUEUE)
    public void processPaymentMessage(
            @Payload PaymentMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("📥 [CONSUMIDOR PAGAMENTO] Mensagem recebida! OrderId='{}', DeliveryTag={}",
                message.orderId(), deliveryTag);

        if (message.simulateError()) {
            // ❌ SIMULAÇÃO DE ERRO: Enviamos NACK com requeue = false!
            // Como a fila tem x-dead-letter-exchange, o RabbitMQ NUNCA descarta a mensagem;
            // ele joga automaticamente na Dead-Letter Queue ('orders.dlq')!
            log.warn("⚠️ [MANUAL NACK] Erro simulado! Executando basicNack(requeue=false). Enviando para DLQ...");
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // ✅ SUCESSO: Processamento normal concluído -> Enviamos ACK manual!
        log.info("✅ [MANUAL ACK] Pagamento de R$ {} processado com sucesso! Executando basicAck...", message.amount());
        channel.basicAck(deliveryTag, false);
    }

    // =========================================================================
    // 2. CONSUMIDOR DA DEAD-LETTER QUEUE (DLQ)
    // =========================================================================
    @RabbitListener(queues = RabbitMQConfig.DEAD_LETTER_QUEUE)
    public void processDeadLetterQueue(
            @Payload PaymentMessage deadMessage,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.error("💀 [DLQ CONSUMIDOR] Mensagem Morta recebida na DLQ! OrderId='{}', Valor=R$ {}",
                deadMessage.orderId(), deadMessage.amount());

        dlqAuditHistory.add(Map.of(
                "source", "DEAD_LETTER_QUEUE (orders.dlq)",
                "deadMessage", deadMessage,
                "reason", "Rejeitada pelo consumidor principal com NACK (requeue=false)"
        ));

        // Damos ACK na DLQ para tirar da fila de mortos após guardar na auditoria
        channel.basicAck(deliveryTag, false);
    }

    // =========================================================================
    // 3. CONSUMIDORES DA TOPIC EXCHANGE (WILDCARDS)
    // =========================================================================
    @RabbitListener(queues = RabbitMQConfig.NOTIFICATION_BR_QUEUE)
    public void processBrTopicQueue(
            @Payload Map<String, Object> message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("🇧🇷 [FILA BRASIL: 'orders.br.*'] Notificação Brasil capturada: {}", message);
        topicReceiptHistory.add(Map.of("queue", "notifications.brazil.queue", "message", message));
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitMQConfig.NOTIFICATION_ALL_QUEUE)
    public void processAllTopicQueue(
            @Payload Map<String, Object> message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("🌐 [FILA GLOBAL: 'orders.#'] Notificação Global capturada: {}", message);
        topicReceiptHistory.add(Map.of("queue", "notifications.all.queue", "message", message));
        channel.basicAck(deliveryTag, false);
    }

    public List<Map<String, Object>> getDlqAuditHistory() {
        return Collections.unmodifiableList(dlqAuditHistory);
    }

    public List<Map<String, Object>> getTopicReceiptHistory() {
        return Collections.unmodifiableList(topicReceiptHistory);
    }
}

