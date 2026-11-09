package com.banco.config;

import com.banco.model.AlertaFraude;
import com.banco.model.Transaccion;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
public class PipelineTopology {

    // Los topics se crean al arrancar: Kafka Streams no parte si el topic de entrada no existe
    @Bean
    public NewTopic topicTransacciones() {
        return TopicBuilder.name("banco-transacciones").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicAlertas() {
        return TopicBuilder.name("banco-alertas").partitions(3).replicas(1).build();
    }

    @Bean
    public KStream<String, Transaccion> kStreamPipeline(StreamsBuilder builder) {

        // Serializadores JSON (para entender los objetos)
        JsonSerde<Transaccion> transaccionSerde = new JsonSerde<>(Transaccion.class);
        JsonSerde<AlertaFraude> alertaSerde = new JsonSerde<>(AlertaFraude.class);

        // PASO 1: LEER (Source) del topic "banco-transacciones"
        KStream<String, Transaccion> streamEntrada = builder.stream(
                "banco-transacciones",
                Consumed.with(Serdes.String(), transaccionSerde));

        // PASO 2: PROCESAR
        // A. Filtrar transacciones irrelevantes (monto < 10)
        KStream<String, Transaccion> transaccionesValidas = streamEntrada
                .filter((key, tx) -> tx.getMonto() > 10)
                .peek((key, tx) -> System.out.println("Transacción procesada: " + tx.getMonto()));

        // B. Detectar fraude (monto > 10.000) -> generar alerta
        KStream<String, AlertaFraude> alertasStream = transaccionesValidas
                .filter((key, tx) -> tx.getMonto() > 10000)
                .mapValues(tx -> new AlertaFraude(
                        tx.getUsuario(),
                        tx.getMonto(),
                        "Transacción inusualmente alta en " + tx.getOrigen(),
                        "ALTA"));

        // PASO 3: ESCRIBIR (Sink) en el topic "banco-alertas"
        alertasStream.to("banco-alertas", Produced.with(Serdes.String(), alertaSerde));

        // Log para ver qué sale
        alertasStream.peek((key, alerta) ->
                System.out.println("ALERTA GENERADA: " + alerta.getMotivo()));

        return streamEntrada;
    }
}
