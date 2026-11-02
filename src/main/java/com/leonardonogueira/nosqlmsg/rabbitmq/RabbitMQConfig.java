package com.leonardonogueira.nosqlmsg.rabbitmq;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    // Nomes de Exchanges
    public static final String DIRECT_EXCHANGE = "orders.direct.exchange";
    public static final String TOPIC_EXCHANGE = "orders.topic.exchange";
    public static final String DEAD_LETTER_EXCHANGE = "orders.dlx";

    // Nomes de Filas
    public static final String PAYMENT_QUEUE = "orders.payment.queue";
    public static final String DEAD_LETTER_QUEUE = "orders.dlq";
    public static final String NOTIFICATION_BR_QUEUE = "notifications.brazil.queue";
    public static final String NOTIFICATION_ALL_QUEUE = "notifications.all.queue";

    // Routing Keys
    public static final String PAYMENT_ROUTING_KEY = "order.payment.process";
    public static final String DLQ_ROUTING_KEY = "orders.dead-letter";

    // =========================================================================
    // 1. CONFIGURAÇÃO DA DEAD-LETTER EXCHANGE (DLX) & FILA DLQ
    // =========================================================================
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DEAD_LETTER_EXCHANGE, true, false);
    }

    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(DLQ_ROUTING_KEY);
    }

    // =========================================================================
    // 2. FILA PRINCIPAL COM ENCAMINHAMENTO AUTOMÁTICO PARA A DLX
    // =========================================================================
    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(DIRECT_EXCHANGE, true, false);
    }

    @Bean
    public Queue paymentQueue() {
        // Vincula a fila principal com a DLX: se der NACK(requeue=false), o RabbitMQ
        // joga automaticamente para a 'orders.dlx' usando a routing key 'orders.dead-letter'!
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE);
        args.put("x-dead-letter-routing-key", DLQ_ROUTING_KEY);

        return new Queue(PAYMENT_QUEUE, true, false, false, args);
    }

    @Bean
    public Binding paymentBinding() {
        return BindingBuilder.bind(paymentQueue())
                .to(directExchange())
                .with(PAYMENT_ROUTING_KEY);
    }

    // =========================================================================
    // 3. TOPIC EXCHANGE & FILAS COM WILDCARDS (* e #)
    // =========================================================================
    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(TOPIC_EXCHANGE, true, false);
    }

    @Bean
    public Queue notificationBrQueue() {
        return QueueBuilder.durable(NOTIFICATION_BR_QUEUE).build();
    }

    @Bean
    public Queue notificationAllQueue() {
        return QueueBuilder.durable(NOTIFICATION_ALL_QUEUE).build();
    }

    // Curinga '*': substitui exatamente 1 palavra (ex: orders.br.pix, orders.br.cartao)
    @Bean
    public Binding topicBrBinding() {
        return BindingBuilder.bind(notificationBrQueue())
                .to(topicExchange())
                .with("orders.br.*");
    }

    // Curinga '#': substitui zero ou varias palavras (recebe tudo que comecar com 'orders.')
    @Bean
    public Binding topicAllBinding() {
        return BindingBuilder.bind(notificationAllQueue())
                .to(topicExchange())
                .with("orders.#");
    }

    // Serializador JSON para podermos ver as mensagens bonitas no painel Web do RabbitMQ
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}

