package com.barrier.webhookdelivery.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.Delivery;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Ordem por subject: duas entregas do mesmo cliente nunca saem juntas; de clientes diferentes, sim.
 *
 * <p><b>O controle de "em voo" é o próprio lease, no banco.</b> Um mapa de chaves em voo por
 * instância seria a quarta ocorrência nesta base do padrão "estado do cluster na memória de uma
 * instância" — bean de tópicos ignorado, cobertura de watchlist por pod, dedup de alerta por pod —
 * e com 5 réplicas não ordenaria nada.
 *
 * <p>Precisa de Postgres real: a exclusão depende de a reivindicação e a consulta de "quem está em
 * voo" enxergarem o mesmo estado transacional.
 */
@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class, properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class DeliveryOrderingIntegrationTest {

  private static final Duration LEASE = Duration.ofMinutes(5);

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  // Scheduler desligado por propriedade, e nao apenas com retry-delay alto: @Scheduled(fixedDelay)
  // dispara a PRIMEIRA execucao imediatamente, independente do delay, e desde que a entrega virou
  // paralela ela reivindica as linhas antes das assercoes. A falha aparecia como "a ordem nao
  // funciona" — diagnostico errado para uma corrida de teste.

  @Autowired DeliveryRepository repository;
  @Autowired org.springframework.transaction.PlatformTransactionManager txManager;
  @Autowired JdbcTemplate jdbc;

  // SQL cru precisa qualificar o schema: a webhook-api usa `webhook`, nao `public` — e o
  // JdbcTemplate nao herda o default-schema do Flyway.

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
  }

  @Test
  void naoReivindicaDuasEntregasDaMesmaChave() {
    grava("subject-A");
    grava("subject-A");

    List<Delivery> lote = reivindica();

    assertThat(lote)
        .as("duas entregas do mesmo subject sairam juntas — a ordem nao esta garantida")
        .hasSize(1);
  }

  @Test
  void chavesDiferentesCorremEmParalelo() {
    grava("subject-A");
    grava("subject-B");
    grava("subject-C");

    assertThat(reivindica()).hasSize(3);
  }

  /** Sem chave não há ordem a preservar: todas podem sair juntas. */
  @Test
  void chaveNulaNaoBloqueiaNinguem() {
    grava(null);
    grava(null);

    assertThat(reivindica()).hasSize(2);
  }

  /**
   * Entrega terminal para de bloquear a chave.
   *
   * <p>Sem isto, um parceiro com endpoint fora do ar travaria aquele subject <b>para sempre</b>, em
   * vez de até esgotar o retry. É a regra que impede a ordem de virar bloqueio eterno.
   */
  @Test
  void entregaTerminalNaoBloqueiaAChave() {
    grava("subject-D");
    jdbc.update("UPDATE webhook_delivery.deliveries SET status = 'DELIVERED' WHERE partition_key = 'subject-D'");
    grava("subject-D");

    assertThat(reivindica()).hasSize(1);
  }

  /**
   * Reivindica dentro de transacao, como o WebhookDeliveryService faz: o claim marca claimed_at
   * nas entidades gerenciadas, e sem transacao ativa o flush nao acontece.
   */
  private List<Delivery> reivindica() {
    return new org.springframework.transaction.support.TransactionTemplate(txManager)
        .execute(status -> repository.claimDue(Instant.now(), 10, LEASE, 100));
  }

  private void grava(String partitionKey) {
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
               (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                status, attempts, next_attempt_at, created_at, partition_key)
        VALUES (?, ?, ?, 'assessment.completed', 'a-1', 'default', 'http://localhost:9000', '{}',
                'PENDING', 0, now() - interval '1 minute', now(), ?)
        """,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        partitionKey);
  }

  /**
   * Contrato: ordem ESTRITA por chave, e não só "nunca em paralelo". A consulta bloqueava apenas
   * enquanto a predecessora tinha posse ativa; ao falhar, ela soltava a posse e entrava em
   * backoff, e a sucessora saía na frente. O receptor via B → A, com a chave que existe para
   * garantir A → B.
   */
  @Test
  void sucessoraEsperaPredecessoraEmBackoff() {
    grava("subject-E", "FAILED", "+ interval '10 minute'", "- interval '2 minute'");
    grava("subject-E", "PENDING", "- interval '1 minute'", "- interval '1 minute'");

    assertThat(reivindica())
        .as("a sucessora saiu enquanto a predecessora esperava a proxima tentativa — ordem quebrada")
        .isEmpty();
  }

  /** Predecessora morta (esgotou as tentativas) libera a sucessora: ordem não vira bloqueio eterno. */
  @Test
  void predecessoraMortaLiberaASucessora() {
    grava("subject-F", "DEAD", "NULL", "- interval '2 minute'");
    grava("subject-F", "PENDING", "- interval '1 minute'", "- interval '1 minute'");

    assertThat(reivindica()).hasSize(1);
  }

  /** Com as duas vencidas, sai a mais antiga — e só ela. */
  @Test
  void entreDuasVencidasSaiAMaisAntiga() {
    grava("subject-G", "PENDING", "- interval '1 minute'", "- interval '1 minute'");
    grava("subject-G", "PENDING", "- interval '3 minute'", "- interval '3 minute'");

    List<Delivery> lote = reivindica();
    assertThat(lote).hasSize(1);
    assertThat(jdbc.queryForObject(
            "SELECT created_at < now() - interval '2 minute' FROM webhook_delivery.deliveries WHERE id = ?",
            Boolean.class, lote.getFirst().id()))
        .isTrue();
  }

  private void grava(String partitionKey, String status, String nextAttemptOffset, String createdOffset) {
    String nextAttempt = "NULL".equals(nextAttemptOffset) ? "NULL" : "now() " + nextAttemptOffset;
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
               (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                status, attempts, next_attempt_at, created_at, partition_key)
        VALUES (?, ?, ?, 'assessment.completed', 'a-1', 'default', 'http://localhost:9000', '{}',
                ?, 0, %s, now() %s, ?)
        """.formatted(nextAttempt, createdOffset),
        UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), status, partitionKey);
  }

  /**
   * Vizinho barulhento: um endpoint fora do ar, com dezenas de entregas em retry, ocupava todos os
   * workers e os outros tenants esperavam. O cap limita quantas entregas do MESMO endpoint ficam
   * em voo — no lote (filtro em memória) e entre ciclos/réplicas (posse ativa no banco).
   */
  @Test
  void capPorEndpointLimitaOLoteDoMesmoEndpoint() {
    UUID barulhento = UUID.randomUUID();
    for (int i = 0; i < 6; i++) gravaParaEndpoint(barulhento);
    UUID quieto = UUID.randomUUID();
    gravaParaEndpoint(quieto);

    List<Delivery> lote = reivindicaComCap(4);

    assertThat(lote.stream().filter(d -> d.endpointId().equals(barulhento))).hasSize(4);
    assertThat(lote.stream().filter(d -> d.endpointId().equals(quieto))).hasSize(1);
  }

  @Test
  void capPorEndpointContaAsQueJaEstaoEmVoo() {
    UUID barulhento = UUID.randomUUID();
    for (int i = 0; i < 6; i++) gravaParaEndpoint(barulhento);
    assertThat(reivindicaComCap(4)).hasSize(4);

    assertThat(reivindicaComCap(4))
        .as("com 4 em voo no banco, o ciclo seguinte nao deveria pegar mais desse endpoint")
        .isEmpty();
    jdbc.update("UPDATE webhook_delivery.deliveries SET status = 'DELIVERED', claimed_at = NULL WHERE claimed_at IS NOT NULL");
    assertThat(reivindicaComCap(4)).hasSize(2);
  }

  private List<Delivery> reivindicaComCap(int maxPerEndpoint) {
    return new org.springframework.transaction.support.TransactionTemplate(txManager)
        .execute(status -> repository.claimDue(Instant.now(), 10, LEASE, maxPerEndpoint));
  }

  private void gravaParaEndpoint(UUID endpointId) {
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
               (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                status, attempts, next_attempt_at, created_at)
        VALUES (?, ?, ?, 'x.y', 'a-1', 'default', 'http://localhost:9000', '{}',
                'PENDING', 0, now() - interval '1 minute', now())
        """,
        UUID.randomUUID(), UUID.randomUUID(), endpointId);
  }
}
