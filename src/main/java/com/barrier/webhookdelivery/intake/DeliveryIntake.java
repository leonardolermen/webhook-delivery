package com.barrier.webhookdelivery.intake;

/** Porta de entrada da biblioteca: quem tem um evento para entregar chama isto. */
public interface DeliveryIntake {

  /**
   * Registra uma entrega para cada endpoint ativo do tenant inscrito em {@code eventType}.
   * Idempotente por {@code (eventId, endpointId)}. <b>Não entrega aqui</b>: a entrega é feita pelo
   * pool, fora da thread de quem chamou — ver {@code WebhookDeliveryService#retryDue}.
   */
  IntakeResult accept(DeliveryRequest request);
}
