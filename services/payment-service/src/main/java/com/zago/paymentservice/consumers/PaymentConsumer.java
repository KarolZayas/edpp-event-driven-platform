package com.zago.paymentservice.consumers;

import com.zago.edpp.events.OrderCreated;
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
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;

    @KafkaListener(topics = {
            "orders.created.v1",
            "payments.retry.5s.v1",
            "payments.retry.30s.v1",
            "payments.retry.5m.v1"
    })
    public void consume(ConsumerRecord<String, OrderCreated> record, Acknowledgment ack) {

        try {
            OrderCreated event = record.value();
            UUID eventId = UUID.fromString(event.getEventId());

            // 🧠 Idempotencia
            if (processedEventRepository.existsById(eventId)) {
                ack.acknowledge();
                return;
            }
            System.out.println("Processing order (payment): " + event.getOrderId());

            // 💥 Simular fallo
            if (Math.random() < 0.7) {
                throw new RuntimeException("Random failure");
            }

            // ✅ éxito
            processedEventRepository.save(new ProcessedEvent(eventId, Instant.now()));
            ack.acknowledge();

        } catch (Exception e) {
            handleRetry(record);
            ack.acknowledge(); // IMPORTANTE: no bloquear
        }
    }

    private void handleRetry(ConsumerRecord<String, OrderCreated> record) {

        int retryCount = getRetryCount(record.headers());

        String nextTopic = switch (retryCount) {
            case 0 -> "payments.retry.5s.v1";
            case 1 -> "payments.retry.30s.v1";
            case 2 -> "payments.retry.5m.v1";
            default -> "payments.dlq.v1";
        };

        ProducerRecord<String, OrderCreated> newRecord =
                new ProducerRecord<>(nextTopic, record.key(), record.value());

        newRecord.headers().add("retry-count",
                String.valueOf(retryCount + 1).getBytes());

        kafkaTemplate.send(newRecord);

        System.out.println("Sent to: " + nextTopic + " retry=" + (retryCount + 1));
    }

    private int getRetryCount(Headers headers) {
        Header header = headers.lastHeader("retry-count");
        return header == null ? 0 : Integer.parseInt(new String(header.value()));
    }
}
