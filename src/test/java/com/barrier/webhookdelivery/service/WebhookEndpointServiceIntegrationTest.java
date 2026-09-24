package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.barrier.webhookdelivery.repository.WebhookEndpointRepository;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class, properties = {"webhook-delivery.scheduler.enabled=false", "webhook-delivery.allow-private-targets=true"})
@Testcontainers
class WebhookEndpointServiceIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired WebhookEndpointService service;
  @Autowired WebhookEndpointRepository repository;
  @Autowired PlatformTransactionManager txManager;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach void limpa() { jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints"); }

  /** O contrato do Barrier: PUT /v1/webhook-endpoints/{tenant} duas vezes = mesmo endpoint, mesmo segredo, URL nova. */
  @Test
  void registerSingleFazUpsertPreservandoSegredo() {
    WebhookEndpoint primeiro = service.registerSingle("t1", "https://acme/v1");
    WebhookEndpoint segundo = service.registerSingle("t1", "https://acme/v2");
    assertThat(segundo.id()).isEqualTo(primeiro.id());
    assertThat(segundo.secret()).isEqualTo(primeiro.secret());
    assertThat(segundo.targetUrl()).isEqualTo("https://acme/v2");
    assertThat(segundo.events()).isEqualTo(WebhookEndpoint.ALL_EVENTS);
    assertThat(service.listByTenant("t1")).hasSize(1);
  }

  @Test
  void rotacaoEDesativacaoPorId() {
    WebhookEndpoint e = service.register("t1", "https://acme/hook", List.of("*"));
    WebhookEndpoint r = service.rotateSecret(e.id()).orElseThrow();
    assertThat(r.secret()).isNotEqualTo(e.secret());
    assertThat(service.resolveSigningMaterial(e.id()).orElseThrow().previousSecret()).isEqualTo(e.secret());
    service.deactivate(e.id());
    assertThat(service.resolveSigningMaterial(e.id())).isEmpty();
  }

  /**
   * Duas chamadas simultâneas para um tenant ainda sem endpoint: cada uma lia "vazio" e inseria o
   * seu. Com dois ids distintos, a idempotência das entregas (event_id, endpoint_id) não impedia
   * o mesmo evento de sair para os dois. O PUT do Barrier é "único por tenant" — tem que ser
   * atômico por tenant.
   */
  @Test
  void registerSingleConcorrenteCriaUmSoEndpoint() throws Exception {
    int chamadas = 12;
    CyclicBarrier largada = new CyclicBarrier(chamadas);
    ExecutorService pool = Executors.newFixedThreadPool(chamadas);
    try {
      List<Future<WebhookEndpoint>> futuros = new ArrayList<>();
      for (int i = 0; i < chamadas; i++) {
        String url = "https://acme/v" + i;
        futuros.add(pool.submit(() -> { largada.await(); return service.registerSingle("t-corrida", url); }));
      }
      for (Future<WebhookEndpoint> f : futuros) f.get();
    } finally {
      pool.shutdownNow();
    }
    assertThat(service.listByTenant("t-corrida"))
        .as("registerSingle concorrente criou mais de um endpoint 'único'")
        .hasSize(1);
  }

  /**
   * Lost update: uma atualização de URL feita sobre uma leitura antiga regravava o segredo
   * anterior a uma rotação (ou reativava um endpoint recém-desativado), porque o save copia todos
   * os campos e nada conferia se a linha mudou no meio. {@code @Transactional} sozinho não evita
   * isso sob READ COMMITTED.
   */
  @Test
  void atualizacaoSobreLeituraAntigaNaoDesfazRotacaoDeSegredo() throws Exception {
    WebhookEndpoint original = service.register("t-lost", "https://acme/v1", List.of("*"));
    CountDownLatch leu = new CountDownLatch(1);
    CountDownLatch rotacionou = new CountDownLatch(1);
    ExecutorService outro = Executors.newSingleThreadExecutor();
    try {
      Future<?> atualizacaoAntiga = outro.submit(() ->
          new TransactionTemplate(txManager).execute(status -> {
            WebhookEndpoint lido = repository.findById(original.id()).orElseThrow();
            leu.countDown();
            try { rotacionou.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return repository.save(lido.withTargetUrl("https://acme/v2"));
          }));
      assertThat(leu.await(10, TimeUnit.SECONDS)).isTrue();
      WebhookEndpoint rotacionado = service.rotateSecret(original.id()).orElseThrow();
      rotacionou.countDown();

      assertThatThrownBy(() -> atualizacaoAntiga.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(ObjectOptimisticLockingFailureException.class);
      assertThat(service.find(original.id()).orElseThrow().secret())
          .as("a atualização baseada em leitura antiga regravou o segredo anterior à rotação")
          .isEqualTo(rotacionado.secret());
    } finally {
      rotacionou.countDown();
      outro.shutdownNow();
    }
  }
}
