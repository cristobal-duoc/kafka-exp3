package com.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Crea el topic "tutorial-particiones" con 3 particiones al arrancar la app.
 * Si el topic ya existe (por ejemplo, creado a mano en Kafka UI), no lo modifica.
 */
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic tutorialParticiones() {
        return TopicBuilder.name("tutorial-particiones").partitions(3).replicas(1).build();
    }
}
