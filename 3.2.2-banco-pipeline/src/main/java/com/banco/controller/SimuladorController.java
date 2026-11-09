package com.banco.controller;

import com.banco.model.Transaccion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/banco")
public class SimuladorController {

    @Autowired
    private KafkaTemplate<String, Transaccion> kafkaTemplate;

    @PostMapping("/transaccion")
    public String enviarTransaccion(@RequestBody Transaccion tx) {
        // Enriquecer con timestamp si falta
        if (tx.getTimestamp() == null) tx.setTimestamp(System.currentTimeMillis());

        // Enviar a Kafka (topic banco-transacciones). Key: usuario, para mantener el orden por usuario
        kafkaTemplate.send("banco-transacciones", tx.getUsuario(), tx);

        return "Enviado: " + tx.getMonto() + " de " + tx.getUsuario();
    }
}
