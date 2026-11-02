package com.leonardonogueira.nosqlmsg.api;

import com.leonardonogueira.nosqlmsg.rabbitmq.RabbitMQOrderConsumer;
import com.leonardonogueira.nosqlmsg.rabbitmq.RabbitMQOrderProducer;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/rabbitmq")
public class RabbitMQLabController {

    private final RabbitMQOrderProducer producer;
    private final RabbitMQOrderConsumer consumer;

    public RabbitMQLabController(RabbitMQOrderProducer producer, RabbitMQOrderConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    /**
     * TESTE 1: Direct Exchange com Manual ACK vs NACK (DLQ)
     * - Se simulateError = false -> O consumidor processa e executa basicAck (mensagem apagada).
     * - Se simulateError = true  -> O consumidor executa basicNack(requeue=false) e o RabbitMQ joga para a DLQ!
     */
    @PostMapping("/payment")
    public Map<String, Object> sendPayment(
            @RequestParam(defaultValue = "PED-7001") String orderId,
            @RequestParam(defaultValue = "450.00") BigDecimal amount,
            @RequestParam(defaultValue = "false") boolean simulateError) {
        return producer.sendPaymentMessage(orderId, amount, simulateError);
    }

    /**
     * TESTE 2: Topic Exchange com Curingas (* e #)
     * Exemplos de routing keys para testar:
     * - 'orders.br.pix'    -> Entrega na Fila Brasil (orders.br.*) E na Fila Global (orders.#)!
     * - 'orders.us.card'   -> NÃO entrega na Fila Brasil, mas entrega na Fila Global (orders.#)!
     * - 'finance.reports'  -> Nenhuma fila recebe (descarta)!
     */
    @PostMapping("/topic")
    public Map<String, Object> sendTopic(
            @RequestParam(defaultValue = "orders.br.pix") String routingKey,
            @RequestParam(defaultValue = "PIX de R$ 100 recebido no Brasil") String content) {
        return producer.sendTopicMessage(routingKey, content);
    }

    /**
     * TESTE 3: Consultar histórico de mensagens mortas na DLQ
     */
    @GetMapping("/dlq-history")
    public List<Map<String, Object>> getDlqHistory() {
        return consumer.getDlqAuditHistory();
    }

    /**
     * TESTE 4: Consultar histórico de entrega das filas de Topic
     */
    @GetMapping("/topic-history")
    public List<Map<String, Object>> getTopicHistory() {
        return consumer.getTopicReceiptHistory();
    }
}

