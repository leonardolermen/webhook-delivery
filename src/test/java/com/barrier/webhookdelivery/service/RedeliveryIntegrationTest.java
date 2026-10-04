package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.client.HmacSigner;
import com.barrier.webhookdelivery.client.WebhookClient;
import com.barrier.webhookdelivery.client.WebhookRequest;
import com.barrier.webhookdelivery.client.WebhookSendResult;
import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.DeliveryQuery;
import com.barrier.webhookdelivery.domain.DeliveryStatus;
import com.barrier.webhookdelivery.domain.RedeliverResult;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.intake.DeliveryRequest;
import com.barrier.webhookdelivery.repository.DeliveryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Reentrega manual contra o Postgres de verdade: o UPDATE condicional, o lote, e a ordem da
 * partição valendo para a entrega que volta. Scheduler desligado — o teste dirige o
 * {@code retryDue()} — e backoff zero, para a entrega morrer em duas chamadas. Requer Docker.
 */
@SpringBootTest(
    classes = com.barrier.webhookdelivery.testapp.TestApplication.class,
    properties = {
      "webhook-delivery.scheduler.enabled=false",
      "webhook-delivery.allow-private-targets=true",
      "webhook-delivery.max-attempts=2",
      "webhook-delivery.base-backoff=0s"
    })
@Testcontainers
@Import(RedeliveryIntegrationTest.ClienteDeTeste.class)
class RedeliveryIntegrationTest {

  private static final Duration LEASE = Duration.ofMinutes(5);

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  /** Registra o que recebeu e responde falha ou sucesso conforme o teste pede. */
  static final class ClienteControlado implements WebhookClient {
    private final AtomicBoolean falha = new AtomicBoolean();
    private final AtomicReference<WebhookRequest> ultima = new AtomicReference<>();

    void falhaSempre() {
      falha.set(true);
    }

    void sucessoSempre() {
      falha.set(false);
    }

    WebhookRequest ultima() {
      return ultima.get();
    }

    @Override
    public WebhookSendResult send(WebhookRequest request) {
      ultima.set(request);
      return falha.get() ? WebhookSendResult.failure(503, "HTTP 503") : WebhookSendResult.ok(200);
    }
  }

  @TestConfiguration
  static class ClienteDeTeste {
    @Bean
    ClienteControlado webhookClient() {
      return new ClienteControlado();
    }
  }

  @Autowired WebhookDeliveryService service;
  @Autowired WebhookEndpointService endpoints;
  @Autowired DeliveryRepository repository;
  @Autowired ClienteControlado client;
  @Autowired TransactionTemplate tx;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
    jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints");
    client.sucessoSempre();
  }

  @Test
  void umaEntregaMortaVoltaASairEUsaOSegredoVigente() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("payment.*"));
    client.falhaSempre();
    service.accept(request("t1", "payment.completed"));
    esgota();
    Delivery morta = unica("t1");
    assertThat(morta.status()).isEqualTo(DeliveryStatus.DEAD);

    endpoints.rotateSecret(endpoint.id());
    client.sucessoSempre();

    assertThat(service.redeliver("t1", morta.id())).isEqualTo(RedeliverResult.SCHEDULED);
    service.retryDue();

    Delivery entregue = unica("t1");
    assertThat(entregue.status()).isEqualTo(DeliveryStatus.DELIVERED);
    assertThat(entregue.lastErrorBeforeRedelivery()).isEqualTo("HTTP 503");
    assertThat(entregue.redeliveredAt()).isNotNull();
    // O instante assinado não é visível ao teste; ele vem no próprio header (t=<epoch>,v1=...).
    // Recalcular com esse t e o segredo VIGENTE (pós-rotação) e bater prova que a assinatura é do
    // segredo de agora, não do que valia quando a entrega morreu.
    WebhookRequest recebida = client.ultima();
    String assinatura = recebida.signature();
    long epoch = Long.parseLong(assinatura.substring(2, assinatura.indexOf(',')));
    String segredoVigente = endpoints.find(endpoint.id()).orElseThrow().secret();
    assertThat(assinatura)
        .isEqualTo(new HmacSigner().sign(recebida.body(), segredoVigente, Instant.ofEpochSecond(epoch)));
  }

  @Test
  void pendingDeliveredEOutroTenantNaoReentregam() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    UUID pendente = insere(endpoint.id(), "PENDING", null, "1 minute");
    UUID entregue = insere(endpoint.id(), "DELIVERED", null, "1 minute");
    UUID morta = insere(endpoint.id(), "DEAD", null, "1 minute");

    assertThat(service.redeliver("t1", pendente)).isEqualTo(RedeliverResult.NOT_REDELIVERABLE);
    assertThat(service.redeliver("t1", entregue)).isEqualTo(RedeliverResult.NOT_REDELIVERABLE);
    assertThat(service.redeliver("t2", morta)).isEqualTo(RedeliverResult.NOT_FOUND);

    assertThat(status(morta)).isEqualTo("DEAD");
  }

  @Test
  void endpointDesativadoNaoReentrega() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    UUID morta = insere(endpoint.id(), "DEAD", null, "1 minute");
    endpoints.deactivate(endpoint.id());

    assertThat(service.redeliver("t1", morta)).isEqualTo(RedeliverResult.NOT_REDELIVERABLE);

    assertThat(status(morta)).isEqualTo("DEAD");
  }

  @Test
  void reentregaEmLoteRespeitaODesdeEOTenant() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    for (int i = 0; i < 3; i++) {
      insere(endpoint.id(), "DEAD", null, "1 minute");
    }
    UUID antiga = insere(endpoint.id(), "DEAD", null, "2 hours");
    UUID deOutroTenant = insereDoTenant("t2", endpoint.id(), "DEAD", "1 minute");
    Instant since = Instant.now().minus(Duration.ofHours(1));

    assertThat(service.redeliverDead("t1", since)).isEqualTo(3);
    assertThat(service.redeliverDead("t1", since)).isZero();

    assertThat(status(antiga)).isEqualTo("DEAD");
    assertThat(status(deOutroTenant)).isEqualTo("DEAD");
    assertThat(jdbc.queryForObject(
            "SELECT count(*) FROM webhook_delivery.deliveries"
                + " WHERE status = 'PENDING' AND last_error_before_redelivery = 'erro antigo'",
            Integer.class))
        .isEqualTo(3);
  }

  @Test
  void entregaReentregueRespeitaAOrdemDaParticao() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    UUID irmaA = insere(endpoint.id(), "PENDING", "p1", "2 minutes");
    UUID irmaB = insere(endpoint.id(), "DEAD", "p1", "1 minute");
    jdbc.update(
        "UPDATE webhook_delivery.deliveries SET next_attempt_at = now() + interval '30 seconds' WHERE id = ?",
        irmaA);

    assertThat(service.redeliver("t1", irmaB)).isEqualTo(RedeliverResult.SCHEDULED);

    // B vence AGORA (a reentrega) e A só daqui a 30s; reivindicando 1 minuto à frente, as duas
    // estão vencidas e B vem PRIMEIRO no ORDER BY nextAttemptAt. Assim o filtro em memória por
    // chave ficaria com B e descartaria A — a única coisa que mantém B fora é a cláusula
    // "irmã mais antiga não terminal" (irma.createdAt < d.createdAt) do selectClaimable. Antes A
    // vencia primeiro, e o teste passava mesmo sem essa cláusula.
    Instant adiante = Instant.now().plus(Duration.ofMinutes(1));
    List<Delivery> primeiro = reivindica(adiante);
    assertThat(primeiro).extracting(Delivery::id).containsExactly(irmaA);

    Delivery entregueA = primeiro.getFirst();
    entregueA.markDelivered();
    assertThat(repository.saveOutcome(entregueA)).isTrue();

    assertThat(reivindica(adiante)).extracting(Delivery::id).containsExactly(irmaB);
  }

  /** FAILED com posse dentro do lease está em voo: zerar por baixo do worker duplicaria a entrega. */
  @Test
  void falhadaEmVooNaoReentrega() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    UUID emVoo = insere(endpoint.id(), "FAILED", null, "1 minute");
    UUID token = UUID.randomUUID();
    jdbc.update(
        "UPDATE webhook_delivery.deliveries SET claimed_at = now(), claim_token = ? WHERE id = ?",
        token, emVoo);

    assertThat(service.redeliver("t1", emVoo)).isEqualTo(RedeliverResult.NOT_REDELIVERABLE);

    assertThat(status(emVoo)).isEqualTo("FAILED");
    assertThat(jdbc.queryForObject(
            "SELECT attempts FROM webhook_delivery.deliveries WHERE id = ?", Integer.class, emVoo))
        .isEqualTo(2);
    assertThat(jdbc.queryForObject(
            "SELECT claim_token FROM webhook_delivery.deliveries WHERE id = ?", UUID.class, emVoo))
        .isEqualTo(token);
  }

  /** Espelho: posse vencida (lease padrão de 2 min, aqui 3 min atrás) é worker morto, não em voo. */
  @Test
  void falhadaComPosseVencidaReentrega() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    UUID abandonada = insere(endpoint.id(), "FAILED", null, "5 minutes");
    jdbc.update(
        "UPDATE webhook_delivery.deliveries"
            + " SET claimed_at = now() - interval '3 minutes', claim_token = ? WHERE id = ?",
        UUID.randomUUID(), abandonada);

    assertThat(service.redeliver("t1", abandonada)).isEqualTo(RedeliverResult.SCHEDULED);

    assertThat(status(abandonada)).isEqualTo("PENDING");
    assertThat(jdbc.queryForObject(
            "SELECT claim_token FROM webhook_delivery.deliveries WHERE id = ?", UUID.class, abandonada))
        .isNull();
  }

  @Test
  void loteRespeitaOMaximo() {
    WebhookEndpoint endpoint = endpoints.register("t1", "http://localhost:9/hook", List.of("*"));
    for (int i = 0; i < 3; i++) {
      insere(endpoint.id(), "DEAD", null, "1 minute");
    }
    Instant since = Instant.now().minus(Duration.ofHours(1));

    assertThat(repository.markDeadRedelivered("t1", since, Instant.now(), 2)).isEqualTo(2);
    assertThat(jdbc.queryForObject(
            "SELECT count(*) FROM webhook_delivery.deliveries WHERE status = 'DEAD'", Integer.class))
        .isEqualTo(1);

    assertThat(repository.markDeadRedelivered("t1", since, Instant.now(), 2)).isEqualTo(1);
  }

  /** {@code retryDue()} até a entrega morrer; limitado, para um defeito não virar teste eterno. */
  private void esgota() {
    for (int i = 0; i < 10; i++) {
      service.retryDue();
      if (unica("t1").status() == DeliveryStatus.DEAD) {
        return;
      }
    }
  }

  private List<Delivery> reivindica(Instant agora) {
    return tx.execute(status -> repository.claimDue(agora, 10, LEASE, 4));
  }

  private Delivery unica(String tenantId) {
    List<Delivery> todas = repository.findByTenant(tenantId, new DeliveryQuery(null, null, null, null, null, 10));
    assertThat(todas).hasSize(1);
    return todas.getFirst();
  }

  private String status(UUID id) {
    return jdbc.queryForObject("SELECT status FROM webhook_delivery.deliveries WHERE id = ?", String.class, id);
  }

  private UUID insere(UUID endpointId, String status, String partitionKey, String idade) {
    return insere("t1", endpointId, status, partitionKey, idade);
  }

  private UUID insereDoTenant(String tenantId, UUID endpointId, String status, String idade) {
    return insere(tenantId, endpointId, status, null, idade);
  }

  private UUID insere(String tenantId, UUID endpointId, String status, String partitionKey, String idade) {
    UUID id = UUID.randomUUID();
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
          (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
           partition_key, status, attempts, last_error, next_attempt_at, created_at)
        VALUES (?, ?, ?, 'payment.completed', 'pay_1', ?, 'http://localhost:9/hook', '{}',
                ?, ?, 2, 'erro antigo', now() - CAST(? AS interval), now() - CAST(? AS interval))
        """,
        id, UUID.randomUUID(), endpointId, tenantId, partitionKey, status, idade, idade);
    return id;
  }

  private static DeliveryRequest request(String tenantId, String eventType) {
    return new DeliveryRequest(tenantId, eventType, UUID.randomUUID(), "pay_1", "pay_1", "{\"id\":\"pay_1\"}", null);
  }
}
