package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DeliveryTest {

  private static Delivery nova() {
    return Delivery.create(
        UUID.randomUUID(), UUID.randomUUID(), "payment.completed", "pay_1",
        "merchant-1", "https://x/webhook", "{}", "pay_1");
  }

  @Test
  void nasceLivrePendenteEVencida() {
    Delivery d = nova();
    assertThat(d.status()).isEqualTo(DeliveryStatus.PENDING);
    assertThat(d.claimedAt()).isNull();
    assertThat(d.nextAttemptAt()).isEqualTo(d.createdAt());
    assertThat(d.eventType()).isEqualTo("payment.completed");
  }

  @Test
  void falhaReagendaAteEsgotarEDepoisMorre() {
    Delivery d = nova();
    d.markFailed("HTTP 500", 2, Instant.now().plusSeconds(30));
    assertThat(d.status()).isEqualTo(DeliveryStatus.FAILED);
    assertThat(d.attempts()).isEqualTo(1);
    d.markFailed("HTTP 500", 2, Instant.now().plusSeconds(60));
    assertThat(d.status()).isEqualTo(DeliveryStatus.DEAD);
    assertThat(d.nextAttemptAt()).isNull();
  }

  /** Endpoint desativado entre a criacao e a tentativa: nao ha o que retentar. */
  @Test
  void markDeadEncerraSemConsumirTentativas() {
    Delivery d = nova();
    d.markDead("endpoint desativado");
    assertThat(d.status()).isEqualTo(DeliveryStatus.DEAD);
    assertThat(d.lastError()).isEqualTo("endpoint desativado");
    assertThat(d.claimedAt()).isNull();
  }
}
