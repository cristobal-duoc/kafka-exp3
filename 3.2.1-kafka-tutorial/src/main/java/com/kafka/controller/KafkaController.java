package com.kafka.controller;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controlador REST que envía mensajes a Kafka (el productor).
 *
 * Demuestra dos conceptos:
 *  - Sin key: los mensajes se reparten entre las particiones 0, 1 y 2.
 *  - Con key: todos los mensajes con la misma key van a la misma partición.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicInteger turno = new AtomicInteger();

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Ejemplos:
     *  SIN KEY: http://localhost:8888/api/kafka/send?message=Mensaje1
     *  CON KEY: http://localhost:8888/api/kafka/send?message=Mensaje1&key=orden-555
     */
    @GetMapping("/send")
    public String sendMessage(@RequestParam String message,
                              @RequestParam(required = false) String key) {
        String topicName = "tutorial-particiones";

        if (key != null && !key.isEmpty()) {
            // Con key: Kafka calcula hash(key) para elegir la partición.
            // Todos los mensajes con la misma key van a la MISMA partición.
            kafkaTemplate.send(topicName, key, message);
            return "Mensaje enviado al topic '" + topicName
                    + "' con KEY: " + key + " | MENSAJE: " + message;
        } else {
            // Sin key: repartimos por turnos entre las particiones 0, 1 y 2.
            // (Si se deja a Kafka, las versiones nuevas mandan varios mensajes
            // seguidos a la MISMA partición: el "sticky partitioner".)
            int particion = turno.getAndIncrement() % 3;
            kafkaTemplate.send(topicName, particion, null, message);
            return "Mensaje enviado al topic '" + topicName
                    + "' (sin key - distribución automática) | MENSAJE: " + message;
        }
    }

    /** Endpoint de verificación. */
    @GetMapping("/health")
    public String health() {
        return "Aplicación de Kafka Tutorial está corriendo en puerto 8888";
    }
}
