package com.barrier.webhookdelivery.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.DeliveryStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Posse na gravação do desfecho. A reivindicação dá um token à entrega; quem grava o resultado
 * precisa apresentar o token vigente. Sem isto, um worker cujo lease venceu gravava por cima do
 * resultado mais novo de quem reivindicou depois — e a entrega saía duas vezes, ou um DELIVERED
 * virava FAILED e saía uma terceira.
 */
@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class, properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class DeliveryOwnershipIntegrationTest {

  private static final Duration LEASE = Duration.ofMinutes(5);

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired DeliveryRepository repository;
  @Autowired PlatformTransactionManager txManager;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
  }

  @Test
  void reivindicacaoDaUmTokenDePosse() {
    grava();
    Delivery d = reivindica().getFirst();
    assertThat(d.claimToken()).isNotNull();
    assertThat(jdbc.queryForObject("SELECT claim_token FROM webhook_delivery.deliveries", UUID.class)).isEqualTo(d.claimToken());
  }

  @Test
  void desfechoComTokenVigenteGravaELiberaAPosse() {
    grava();
    Delivery d = reivindica().getFirst();
    d.markDelivered();

    assertThat(repository.saveOutcome(d)).isTrue();
    assertThat(jdbc.queryForMap("SELECT status, claimed_at, claim_token, attempts FROM webhook_delivery.deliveries"))
        .containsEntry("status", "DELIVERED")
        .containsEntry("claimed_at", null)
        .containsEntry("claim_token", null)
        .containsEntry("attempts", 1);
  }

  @Test
  void desfechoComTokenVencidoNaoSobrescreveOResultadoMaisNovo() {
    grava();
    Delivery primeiroWorker = reivindica().getFirst();
    // O lease do primeiro venceu (a instância travou no POST); outro worker reivindica.
    jdbc.update("UPDATE webhook_delivery.deliveries SET claimed_at = now() - interval '1 hour'");
    Delivery segundoWorker = reivindica().getFirst();
    assertThat(segundoWorker.claimToken()).isNotEqualTo(primeiroWorker.claimToken());

    segundoWorker.markDelivered();
    assertThat(repository.saveOutcome(segundoWorker)).isTrue();

    primeiroWorker.markFailed("timeout", 5, Instant.now().plusSeconds(30));
    assertThat(repository.saveOutcome(primeiroWorker))
        .as("o worker com posse vencida gravou por cima do resultado mais novo")
        .isFalse();
    assertThat(jdbc.queryForObject("SELECT status FROM webhook_delivery.deliveries", String.class)).isEqualTo("DELIVERED");
  }

  @Test
  void desfechoLibera_ANovaReivindicacaoQuandoFalha() {
    grava();
    Delivery d = reivindica().getFirst();
    d.markFailed("HTTP 500", 5, Instant.now().minusSeconds(1));
    assertThat(repository.saveOutcome(d)).isTrue();

    List<Delivery> novo = reivindica();
    assertThat(novo).hasSize(1);
    assertThat(novo.getFirst().status()).isEqualTo(DeliveryStatus.FAILED);
    assertThat(novo.getFirst().claimToken()).isNotEqualTo(d.claimToken());
  }

  private List<Delivery> reivindica() {
    return new TransactionTemplate(txManager).execute(status -> repository.claimDue(Instant.now(), 10, LEASE, 100));
  }

  private void grava() {
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
               (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                status, attempts, next_attempt_at, created_at)
        VALUES (?, ?, ?, 'x.y', 'a-1', 'default', 'http://localhost:9000', '{}',
                'PENDING', 0, now() - interval '1 minute', now())
        """,
        UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
  }
}
