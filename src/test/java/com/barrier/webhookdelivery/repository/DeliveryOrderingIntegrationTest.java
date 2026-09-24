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
        .execute(status -> repository.claimDue(Instant.now(), 10, LEASE));
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
}
