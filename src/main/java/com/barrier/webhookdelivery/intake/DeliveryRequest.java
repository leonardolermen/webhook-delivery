package com.barrier.webhookdelivery.intake;

import java.util.Objects;
import java.util.UUID;

/**
 * Pedido de entrega de um evento aos endpoints de um tenant.
 *
 * @param tenantId dono do evento; é dele que saem os endpoints e os segredos
 * @param eventType nome canônico ({@code payment.completed}); é o que os endpoints filtram
 * @param eventId idempotência: o mesmo evento nunca gera duas entregas para o mesmo endpoint
 * @param aggregateId id do agregado de origem, só para rastreio; a lib não interpreta
 * @param partitionKey chave de ordenação estrita: uma entrega só sai depois que as mais antigas da
 *     mesma chave chegaram a estado terminal (entregue ou morta), backoff incluído.
 *     {@code null} = sem ordem exigida — fail-open, o desconhecido não trava a fila
 * @param payload corpo exato que será assinado e entregue; a lib não o altera nem normaliza
 * @param correlationId id da requisição de origem, para o log; pode ser {@code null}
 */
public record DeliveryRequest(
    String tenantId,
    String eventType,
    UUID eventId,
    String aggregateId,
    String partitionKey,
    String payload,
    String correlationId) {

  public DeliveryRequest {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(payload, "payload");
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId obrigatório");
    }
    if (eventType == null || eventType.isBlank()) {
      throw new IllegalArgumentException("eventType obrigatório");
    }
    if (aggregateId == null || aggregateId.isBlank()) {
      throw new IllegalArgumentException("aggregateId obrigatório");
    }
  }
}
