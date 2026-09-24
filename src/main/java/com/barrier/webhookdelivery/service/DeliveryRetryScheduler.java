package com.barrier.webhookdelivery.service;

import org.springframework.scheduling.annotation.Scheduled;

/** Dispara periodicamente o reprocessamento de entregas vencidas. */
public class DeliveryRetryScheduler {

  private final WebhookDeliveryService service;

  public DeliveryRetryScheduler(WebhookDeliveryService service) {
    this.service = service;
  }

  @Scheduled(fixedDelayString = "${webhook-delivery.retry-delay-ms:5000}")
  public void retry() {
    service.retryDue();
  }
}
