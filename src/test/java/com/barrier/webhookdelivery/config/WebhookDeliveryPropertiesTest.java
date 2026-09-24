package com.barrier.webhookdelivery.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WebhookDeliveryPropertiesTest {

  @Test
  void defaultsSaoOsDoBarrier() {
    WebhookDeliveryProperties p =
        new WebhookDeliveryProperties(0, null, 0, 0, null, null, null, null, null, null, null, null);
    assertThat(p.workers()).isEqualTo(3);
    assertThat(p.lease()).isEqualTo(Duration.ofMinutes(2));
    assertThat(p.retryDelayMs()).isEqualTo(5000);
    assertThat(p.maxAttempts()).isEqualTo(5);
    assertThat(p.baseBackoff()).isEqualTo(Duration.ofSeconds(30));
    assertThat(p.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
    assertThat(p.readTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(p.secretRotationOverlap()).isEqualTo(Duration.ofHours(24));
    assertThat(p.headers().prefix()).isEqualTo("X-Webhook");
    assertThat(p.correlationMdcKey()).isEqualTo("correlationId");
    assertThat(p.scheduler().enabled()).isTrue();
    assertThat(p.flyway().baselineOnMigrate()).isFalse();
    assertThat(p.flyway().baselineVersion()).isEqualTo("1");
  }

  @Test
  void headersDerivamDoPrefixo() {
    WebhookDeliveryProperties.Headers h = new WebhookDeliveryProperties.Headers("X-Barrier");
    assertThat(h.signature()).isEqualTo("X-Barrier-Signature");
    assertThat(h.previousSignature()).isEqualTo("X-Barrier-Signature-Previous");
    assertThat(h.eventId()).isEqualTo("X-Barrier-Event-Id");
    assertThat(h.eventType()).isEqualTo("X-Barrier-Event-Type");
  }
}
