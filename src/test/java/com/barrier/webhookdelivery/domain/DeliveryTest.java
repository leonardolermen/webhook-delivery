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

  /**
   * A coluna {@code last_error} tem 500 caracteres. Um detalhe maior (URL longa numa
   * ResourceAccessException, corpo de erro do parceiro) fazia o save falhar, e a falha deixava
   * contador e backoff sem persistir: a entrega voltava com o contador antigo a cada lease.
   */
  @Test
  void erroLongoEhTruncadoParaCaberNaColuna() {
    Delivery d = nova();
    d.markFailed("x".repeat(2000), 5, Instant.now().plusSeconds(30));
    assertThat(d.lastError()).hasSize(Delivery.MAX_ERROR_LENGTH);
    d.markDead("y".repeat(2000));
    assertThat(d.lastError()).hasSize(Delivery.MAX_ERROR_LENGTH);
  }

  @Test
  void erroCurtoNaoEhAlterado() {
    Delivery d = nova();
    d.markFailed("HTTP 500", 5, Instant.now().plusSeconds(30));
    assertThat(d.lastError()).isEqualTo("HTTP 500");
    d.markFailed(null, 5, Instant.now().plusSeconds(30));
    assertThat(d.lastError()).isNull();
  }
}
