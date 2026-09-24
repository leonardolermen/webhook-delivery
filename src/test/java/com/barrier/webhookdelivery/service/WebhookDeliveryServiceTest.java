package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.client.HmacSigner;
import com.barrier.webhookdelivery.client.WebhookClient;
import com.barrier.webhookdelivery.client.WebhookRequest;
import com.barrier.webhookdelivery.client.WebhookSendResult;
import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.DeliveryStatus;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.intake.DeliveryRequest;
import com.barrier.webhookdelivery.repository.DeliveryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * A reivindicação tem que caber nos workers. Antes {@code retryDue} reivindicava 100 de uma vez
 * com 3 permissões: a centésima só começava depois de ~34 rodadas de até 12 s, muito além do
 * lease de 2 min — outra réplica a reivindicava e reenviava enquanto esta ainda ia enviá-la.
 */
class WebhookDeliveryServiceTest {

  /** Fake em memória do repositório: o que está em teste é o tamanho da reivindicação. */
  static final class RepositorioEmMemoria implements DeliveryRepository {
    final List<Delivery> dados = new CopyOnWriteArrayList<>();
    final List<Integer> limitesPedidos = new CopyOnWriteArrayList<>();
    final List<Integer> capsPorEndpointPedidos = new CopyOnWriteArrayList<>();

    @Override public boolean saveIfAbsent(Delivery d) { dados.add(d); return true; }
    @Override public boolean existsByEventId(UUID eventId) { return dados.stream().anyMatch(d -> d.eventId().equals(eventId)); }
    @Override public Optional<Delivery> findById(UUID id) { return dados.stream().filter(d -> d.id().equals(id)).findFirst(); }

    @Override
    public synchronized List<Delivery> claimDue(Instant now, int limit, Duration lease, int maxPerEndpoint) {
      limitesPedidos.add(limit);
      capsPorEndpointPedidos.add(maxPerEndpoint);
      List<Delivery> lote = new ArrayList<>();
      for (Delivery d : dados) {
        if (lote.size() >= limit) break;
        boolean vencida = d.status() == DeliveryStatus.PENDING || d.status() == DeliveryStatus.FAILED;
        boolean livre = d.claimedAt() == null || d.claimedAt().isBefore(now.minus(lease));
        if (vencida && livre && !d.nextAttemptAt().isAfter(now)) {
          d.claim(now, UUID.randomUUID());
          lote.add(d);
        }
      }
      return lote;
    }

    @Override
    public synchronized boolean saveOutcome(Delivery d) {
      return dados.stream().anyMatch(x -> x.id().equals(d.id()) && x.claimToken() != null && x.claimToken().equals(d.claimToken()));
    }
  }

  /** Cliente que conta quantos envios estão em voo ao mesmo tempo. */
  static final class ClienteContador implements WebhookClient {
    final AtomicInteger emVoo = new AtomicInteger();
    final AtomicInteger picoEmVoo = new AtomicInteger();
    final AtomicInteger envios = new AtomicInteger();

    @Override
    public WebhookSendResult send(WebhookRequest request) {
      int agora = emVoo.incrementAndGet();
      picoEmVoo.accumulateAndGet(agora, Math::max);
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        emVoo.decrementAndGet();
      }
      envios.incrementAndGet();
      return WebhookSendResult.ok(200);
    }
  }

  /** Sem banco: a transação é só o contorno que {@code claimDue} exige. */
  static final class SemTransacao implements PlatformTransactionManager {
    @Override public TransactionStatus getTransaction(TransactionDefinition d) { return new SimpleTransactionStatus(); }
    @Override public void commit(TransactionStatus s) {}
    @Override public void rollback(TransactionStatus s) {}
  }

  @Test
  void reivindicaNoMaximoOQueCabeNosWorkersMasEntregaOLoteInteiro() {
    int workers = 3;
    WebhookDeliveryProperties props = new WebhookDeliveryProperties(
        workers, null, 0, 0, null, null, null, true, 0, null, null, null, null, null);
    var endpoints = new WebhookEndpointServiceTest.RepositorioEmMemoria();
    WebhookEndpointService endpointService = new WebhookEndpointService(endpoints, props);
    WebhookEndpoint endpoint = endpointService.register("t1", "http://localhost:1/hook", List.of("*"));
    RepositorioEmMemoria repo = new RepositorioEmMemoria();
    ClienteContador cliente = new ClienteContador();
    try (WebhookDeliveryService service = new WebhookDeliveryService(
        repo, endpointService, cliente, new HmacSigner(), props, new TransactionTemplate(new SemTransacao()))) {

      for (int i = 0; i < 10; i++) {
        service.accept(new DeliveryRequest("t1", "x.y", UUID.randomUUID(), "a" + i, null, "{}", null));
      }
      assertThat(repo.dados).hasSize(10);

      int tentadas = service.retryDue();

      assertThat(tentadas).isEqualTo(10);
      assertThat(cliente.envios.get()).isEqualTo(10);
      assertThat(repo.limitesPedidos)
          .as("cada reivindicacao tem que caber nas permissoes livres")
          .allSatisfy(limite -> assertThat(limite).isBetween(1, workers));
      assertThat(cliente.picoEmVoo.get()).isLessThanOrEqualTo(workers);
      assertThat(repo.capsPorEndpointPedidos).as("o cap por endpoint vem da propriedade").allMatch(c -> c == 4);
      assertThat(repo.dados).allSatisfy(d -> assertThat(d.status()).isEqualTo(DeliveryStatus.DELIVERED));
    }
  }

  /** Um ciclo sem nada vencido não gira em falso nem reivindica. */
  @Test
  void semNadaVencidoRetornaZero() {
    WebhookDeliveryProperties props = new WebhookDeliveryProperties(
        3, null, 0, 0, null, null, null, true, 0, null, null, null, null, null);
    RepositorioEmMemoria repo = new RepositorioEmMemoria();
    try (WebhookDeliveryService service = new WebhookDeliveryService(
        repo, new WebhookEndpointService(new WebhookEndpointServiceTest.RepositorioEmMemoria(), props),
        new ClienteContador(), new HmacSigner(), props, new TransactionTemplate(new SemTransacao()))) {
      assertThat(service.retryDue()).isZero();
      assertThat(repo.limitesPedidos).hasSize(1);
    }
  }

  /** Cliente que trava o PRIMEIRO envio até ser liberado; os demais respondem na hora. */
  static final class ClienteComUmTravado implements WebhookClient {
    final CountDownLatch libera = new CountDownLatch(1);
    final AtomicInteger envios = new AtomicInteger();
    final AtomicInteger primeiro = new AtomicInteger();

    @Override
    public WebhookSendResult send(WebhookRequest request) {
      if (primeiro.compareAndSet(0, 1)) {
        try { libera.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      }
      envios.incrementAndGet();
      return WebhookSendResult.ok(200);
    }
  }

  /**
   * Reposição contínua: um destino lento não pode segurar os slots dos outros. Antes o ciclo
   * esperava o sublote inteiro terminar para reivindicar de novo — com um parceiro em timeout de
   * 12 s, os outros workers ficavam ociosos esse tempo todo.
   */
  @Test
  void umEnvioTravadoNaoImpedeOsOutrosDeSeremEntregues() throws Exception {
    int workers = 3;
    WebhookDeliveryProperties props = new WebhookDeliveryProperties(
        workers, null, 0, 0, null, null, null, true, 0, null, null, null, null, null);
    WebhookEndpointService endpointService = new WebhookEndpointService(new WebhookEndpointServiceTest.RepositorioEmMemoria(), props);
    endpointService.register("t1", "http://localhost:1/hook", List.of("*"));
    RepositorioEmMemoria repo = new RepositorioEmMemoria();
    ClienteComUmTravado cliente = new ClienteComUmTravado();
    ExecutorService ciclo = Executors.newSingleThreadExecutor();
    try (WebhookDeliveryService service = new WebhookDeliveryService(
        repo, endpointService, cliente, new HmacSigner(), props, new TransactionTemplate(new SemTransacao()))) {
      for (int i = 0; i < 10; i++) {
        service.accept(new DeliveryRequest("t1", "x.y", UUID.randomUUID(), "a" + i, null, "{}", null));
      }

      Future<Integer> tentadas = ciclo.submit(service::retryDue);

      Awaitility.await().atMost(Duration.ofSeconds(5))
          .alias("com um envio travado, os outros nove deveriam ter sido entregues")
          .untilAtomic(cliente.envios, org.hamcrest.Matchers.equalTo(9));
      cliente.libera.countDown();
      assertThat(tentadas.get(5, TimeUnit.SECONDS)).isEqualTo(10);
      assertThat(repo.dados).allSatisfy(d -> assertThat(d.status()).isEqualTo(DeliveryStatus.DELIVERED));
    } finally {
      cliente.libera.countDown();
      ciclo.shutdownNow();
    }
  }
}
