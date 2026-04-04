package com.zago.paymentservice.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zago.edpp.events.PaymentCompleted;
import com.zago.edpp.events.PaymentStatus;
import com.zago.paymentservice.entity.ProcessedEvent;
import com.zago.paymentservice.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, PaymentCompleted> kafkaTemplateAvro;
    private final KafkaTemplate<String, String> kafkaTemplateJson;

    @KafkaListener(topics = {
            "orders.created.v1",
            "payments.retry.5s.v1",
            "payments.retry.30s.v1",
            "payments.retry.5m.v1"
    })
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {

        try {
            JsonNode envelope = objectMapper.readTree(record.value());
            String payloadString = envelope.get("payload").asText();
            JsonNode event = objectMapper.readTree(payloadString);
            UUID eventId = UUID.fromString(event.get("eventId").asText());

            // 🧠 Idempotencia
            if (processedEventRepository.existsById(eventId)) {
                ack.acknowledge();
                return;
            }
            System.out.println("Processing order (payment): " + event.get("orderId").asText());

            // 💥 Simular fallo

            // 💳 lógica de negocio
            //processPayment(event);

            // 🧬 transformar a Avro
            var avroEvent = mapToAvro(event);

            // 🚀 publicar en Kafka (Avro)
            kafkaTemplateAvro.send("payments.completed.v1", avroEvent);

            // ✅ éxito
            processedEventRepository.save(new ProcessedEvent(eventId, Instant.now()));
            ack.acknowledge();

        } catch (Exception e) {
            System.out.println("Error processing order (payment): " + e.getMessage());
            handleRetry(record);
            ack.acknowledge(); // IMPORTANTE: no bloquear
        }
    }

    private void handleRetry(ConsumerRecord<String, String> record) {

        int retryCount = getRetryCount(record.headers());

        String nextTopic = switch (retryCount) {
            case 0 -> "payments.retry.5s.v1";
            case 1 -> "payments.retry.30s.v1";
            case 2 -> "payments.retry.5m.v1";
            default -> "payments.dlq.v1";
        };

        ProducerRecord<String, String> newRecord =
                new ProducerRecord<>(nextTopic, record.key(), record.value());

        newRecord.headers().add("retry-count",
                String.valueOf(retryCount + 1).getBytes());

        kafkaTemplateJson.send(newRecord);

        System.out.println("Sent to: " + nextTopic + " retry=" + (retryCount + 1));
    }

    private int getRetryCount(Headers headers) {
        Header header = headers.lastHeader("retry-count");
        return header == null ? 0 : Integer.parseInt(new String(header.value()));
    }

    private PaymentCompleted mapToAvro(JsonNode event) {

        return PaymentCompleted.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("PaymentCompleted")
                .setEventVersion("v1")
                .setOccurredAt(System.currentTimeMillis())

                .setOrderId(event.get("orderId").asText())
                .setUserId(event.get("userId").asText())
                .setAmount(event.get("amount").asDouble())

                .setCurrency("MXN")
                .setStatus(PaymentStatus.SUCCESS)

                .setPaymentMethod("CARD")

                .setMetadata(Map.of(
                        "source", "payment-service",
                        "env", "local"
                ))

                .build();
    }
}
