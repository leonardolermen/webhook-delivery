package com.barrier.webhookdelivery.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.barrier.webhookdelivery.domain.Delivery;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Exclusão da reivindicação entre réplicas — a janela que sobrava depois do {@code SKIP LOCKED}.
 *
 * <p><b>O que já estava coberto, e por um motivo que não é óbvio:</b> {@code selectClaimable} aplica
 * {@code FOR UPDATE} a <b>todas</b> as linhas que retorna, inclusive as que o filtro em memória
 * descarta em seguida. Duas entregas do mesmo subject já existentes vêm as duas no resultado, ficam
 * as duas travadas até o commit, e a outra réplica pula ambas. O caso mais comum, portanto, nunca
 * esteve furado — mas a proteção é efeito colateral de um filtro que roda depois, e não decisão
 * de ninguém.
 *
 * <p><b>O que não estava:</b> a linha que <i>nasce</i> depois do {@code SELECT} da outra réplica.
 * Quem chama {@code accept} insere entregas continuamente enquanto um ciclo de claim está em voo;
 * essa linha nova não foi travada por ninguém, e sob {@code READ COMMITTED} o {@code claimed_at}
 * ainda não commitado da irmã é invisível. A réplica seguinte conclui "ninguém em voo" e
 * reivindica: as duas entregas do mesmo subject saem em paralelo, que é exatamente a garantia que a
 * chave de partição existe para dar. Não é caso de laboratório — é o fluxo normal com tráfego.
 *
 * <p>Só reproduz com duas transações <b>de verdade</b> encavaladas, por isso os latches: a primeira
 * segura a transação aberta enquanto a linha nova aparece e a segunda tenta.
 */
@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class, properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class DeliveryClaimExclusionIntegrationTest {

  private static final Duration LEASE = Duration.ofMinutes(5);

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  // Ver o racional em DeliveryOrderingIntegrationTest: o @Scheduled reivindica antes das asserções.

  @Autowired DeliveryRepository repository;
  @Autowired PlatformTransactionManager txManager;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
  }

  @Test
  void naoReivindicaEntregaQueNasceuComOutroClaimEmVoo() throws Exception {
    grava("subject-X");

    CountDownLatch aReivindicou = new CountDownLatch(1);
    CountDownLatch bTerminou = new CountDownLatch(1);
    ExecutorService podA = Executors.newSingleThreadExecutor();
    try {
      Future<List<Delivery>> futuroA =
          podA.submit(
              () ->
                  tx().execute(
                          status -> {
                            List<Delivery> lote = repository.claimDue(Instant.now(), 10, LEASE);
                            aReivindicou.countDown();
                            // Segura a transação ABERTA: é este intervalo — claim feito, commit
                            // pendente — que a outra réplica enxergava como "chave livre".
                            aguarda(bTerminou);
                            return lote;
                          }));

      assertThat(aReivindicou.await(10, TimeUnit.SECONDS)).isTrue();
      // A segunda entrega do MESMO subject chega agora, por quem chama accept: não existia quando
      // o pod A fez o SELECT, então nenhum FOR UPDATE a alcançou.
      grava("subject-X");

      List<Delivery> loteB = tx().execute(status -> repository.claimDue(Instant.now(), 10, LEASE));
      bTerminou.countDown();

      List<Delivery> loteA = futuroA.get(10, TimeUnit.SECONDS);

      assertThat(loteA).as("o primeiro pod deveria reivindicar uma entrega").hasSize(1);
      assertThat(loteB)
          .as("o segundo pod reivindicou a irmã com o claim do primeiro em voo — a ordem quebra")
          .isEmpty();
    } finally {
      bTerminou.countDown();
      podA.shutdownNow();
    }
  }

  /**
   * Duas entregas já existentes: coberto pelo {@code FOR UPDATE} sobre todos os candidatos
   * retornados, antes mesmo da trava. Fica como registro de que essa proteção existe — se um dia o
   * filtro em memória virar filtro em SQL, os candidatos descartados deixam de ser travados e este
   * teste é que vai avisar.
   */
  @Test
  void naoReivindicaIrmaJaExistenteComClaimEmVoo() throws Exception {
    grava("subject-Z");
    grava("subject-Z");

    CountDownLatch aReivindicou = new CountDownLatch(1);
    CountDownLatch bTerminou = new CountDownLatch(1);
    ExecutorService podA = Executors.newSingleThreadExecutor();
    try {
      Future<List<Delivery>> futuroA =
          podA.submit(
              () ->
                  tx().execute(
                          status -> {
                            List<Delivery> lote = repository.claimDue(Instant.now(), 10, LEASE);
                            aReivindicou.countDown();
                            aguarda(bTerminou);
                            return lote;
                          }));

      assertThat(aReivindicou.await(10, TimeUnit.SECONDS)).isTrue();
      List<Delivery> loteB = tx().execute(status -> repository.claimDue(Instant.now(), 10, LEASE));
      bTerminou.countDown();

      assertThat(futuroA.get(10, TimeUnit.SECONDS)).hasSize(1);
      assertThat(loteB).isEmpty();
    } finally {
      bTerminou.countDown();
      podA.shutdownNow();
    }
  }

  /**
   * Fora de transação o advisory lock é liberado no auto-commit, e a exclusão sumiria sem nenhum
   * sinal — o modo de falha silencioso que esta frente existe para eliminar. Recusar alto é a
   * correção.
   *
   * <p>Chega como {@code InvalidDataAccessApiUsageException} e não como {@code IllegalStateException}
   * porque {@code @Repository} traduz exceção de acesso a dados; o tipo original fica na causa. A
   * asserção é sobre a causa e a mensagem de propósito — é a mensagem que diz ao próximo
   * desenvolvedor por que o método recusou.
   */
  @Test
  void recusaReivindicarForaDeTransacao() {
    assertThatThrownBy(() -> repository.claimDue(Instant.now(), 10, LEASE))
        .isInstanceOf(InvalidDataAccessApiUsageException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining("precisa rodar dentro de uma transação");
  }

  /** Liberada a trava, o ciclo seguinte reivindica normalmente: a exclusão atrasa, não descarta. */
  @Test
  void reivindicaNoCicloSeguinteDepoisDaTravaLiberada() {
    grava("subject-Y");
    grava("subject-Y");

    assertThat(reivindica()).hasSize(1);
    jdbc.update("UPDATE webhook_delivery.deliveries SET status = 'DELIVERED' WHERE claimed_at IS NOT NULL");

    assertThat(reivindica()).as("a segunda entrega ficou presa depois da primeira concluir").hasSize(1);
  }

  private static void aguarda(CountDownLatch latch) {
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private TransactionTemplate tx() {
    return new TransactionTemplate(txManager);
  }

  private List<Delivery> reivindica() {
    return tx().execute(status -> repository.claimDue(Instant.now(), 10, LEASE));
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
