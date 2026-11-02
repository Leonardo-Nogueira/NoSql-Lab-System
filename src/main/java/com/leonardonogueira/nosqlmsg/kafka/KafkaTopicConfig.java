package com.leonardonogueira.nosqlmsg.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ORDERS_TOPIC = "orders-events-topic";

    /**
     * Cria automaticamente o topico no Apache Kafka com 3 PARTICÕES:
     * - Permite paralelismo com ate 3 consumidores por Consumer Group!
     * - Garante ordenacao estrita de mensagens com a mesma chave (key) dentro de cada particao.
     */
    @Bean
    public NewTopic ordersEventsTopic() {
        return TopicBuilder.name(ORDERS_TOPIC)
                .partitions(3)
                .replicas(1) // 1 replica pois estamos com 1 broker local
                .build();
    }
}

