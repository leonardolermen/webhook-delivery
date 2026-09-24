package com.barrier.webhookdelivery.service;

import com.barrier.webhookdelivery.client.HmacSigner;
import com.barrier.webhookdelivery.client.WebhookClient;
import com.barrier.webhookdelivery.client.WebhookRequest;
import com.barrier.webhookdelivery.client.WebhookSendResult;
import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.SigningMaterial;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.intake.DeliveryIntake;
import com.barrier.webhookdelivery.intake.DeliveryRequest;
import com.barrier.webhookdelivery.intake.IntakeResult;
import com.barrier.webhookdelivery.observability.Correlation;
import com.barrier.webhookdelivery.repository.DeliveryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Entrega os eventos aceitos por {@link #accept(DeliveryRequest)} ao endpoint de cada tenant.
 *
 * <p>Idempotência por {@code (eventId, endpointId)}. A entrega é assinada com HMAC; falhas
 * reagendam com backoff exponencial até esgotar as tentativas.
 */
public class WebhookDeliveryService implements DeliveryIntake, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryService.class);
  private static final int RETRY_BATCH = 100;

  private final DeliveryRepository repository;
  private final WebhookEndpointService endpoints;
  private final WebhookClient client;
  private final HmacSigner signer;
  private final WebhookDeliveryProperties properties;
  private final TransactionTemplate transactionTemplate;
  private final Duration lease;

  /** Virtual threads: a tarefa fica bloqueada esperando o destino, nao uma thread de plataforma. */
  private final ExecutorService entregas = Executors.newVirtualThreadPerTaskExecutor();

  private final Semaphore permissoes;

  public WebhookDeliveryService(
      DeliveryRepository repository,
      WebhookEndpointService endpoints,
      WebhookClient client,
      HmacSigner signer,
      WebhookDeliveryProperties properties,
      TransactionTemplate transactionTemplate) {
    this.repository = repository;
    this.endpoints = endpoints;
    this.client = client;
    this.signer = signer;
    this.properties = properties;
    this.transactionTemplate = transactionTemplate;
    this.permissoes = new Semaphore(properties.workers());
    // Precisa ser maior que o pior caso de uma tentativa (connect + read timeout do cliente);
    // curta demais faz outro worker reenviar enquanto o POST original ainda está em voo.
    this.lease = properties.lease();
  }

  @Override
  public IntakeResult accept(DeliveryRequest request) {
    return Correlation.call(properties.correlationMdcKey(), request.correlationId(), () -> registrar(request));
  }

  private IntakeResult registrar(DeliveryRequest request) {
    List<WebhookEndpoint> inscritos =
        endpoints.listByTenant(request.tenantId()).stream()
            .filter(WebhookEndpoint::active)
            .filter(e -> e.subscribedTo(request.eventType()))
            .toList();
    if (inscritos.isEmpty()) {
      // Não entregar é o desfecho correto — entregar no lugar errado é irreversível, e o fato
      // continua registrado no sistema de origem.
      log.warn("Tenant {} sem endpoint inscrito em {}; evento {} não entregue", request.tenantId(), request.eventType(), request.eventId());
      return IntakeResult.NONE;
    }
    int criadas = 0;
    for (WebhookEndpoint endpoint : inscritos) {
      boolean criada = repository.saveIfAbsent(Delivery.create(request.eventId(), endpoint.id(), request.eventType(),
          request.aggregateId(), request.tenantId(), endpoint.targetUrl(), request.payload(), request.partitionKey()));
      if (criada) {
        criadas++;
      } else {
        // (event_id, endpoint_id) já existe: repetição de quem chama, ou corrida entre réplicas.
        log.debug("Entrega do evento {} para o endpoint {} já registrada; ignorando", request.eventId(), endpoint.id());
      }
    }
    // A entrega NÃO acontece aqui, de propósito — ver o comentário original: quem entrega é o
    // retryDue(), pelo pool, fora da thread de quem chamou.
    return new IntakeResult(inscritos.size(), criadas);
  }

  /**
   * Reprocessa entregas vencidas (agendado). Retorna quantas foram tentadas.
   *
   * <p>A reivindicação acontece numa transação curta; os POSTs ficam <b>fora</b> dela. Antes o
   * método só lia (`findDue`) e saía postando: réplicas concorrentes entregavam o mesmo veredito de
   * KYC ao cliente, e mesmo com uma instância só o scheduler competia com o próprio listener do
   * Kafka por entregas recém-criadas (ver migration V003).
   */
  public int retryDue() {
    int tentadas = 0;
    List<CompletableFuture<Void>> emVoo = new ArrayList<>();
    while (tentadas < RETRY_BATCH) {
      // Reposição contínua: espera UMA permissão livre (um worker terminou), pega as demais que
      // estiverem livres e reivindica só isso. Um parceiro em timeout segura o worker dele, não os
      // outros — antes o ciclo esperava o sublote inteiro, e os slots livres ficavam ociosos até o
      // mais lento responder. E cada entrega começa assim que é reivindicada: o lease só precisa
      // cobrir UMA tentativa, não 100/workers rodadas.
      permissoes.acquireUninterruptibly();
      int capacidade = Math.min(1 + permissoes.drainPermits(), RETRY_BATCH - tentadas);
      List<Delivery> due =
          transactionTemplate.execute(
              status -> repository.claimDue(Instant.now(), capacidade, lease, properties.maxInFlightPerEndpoint()));
      int reivindicadas = due == null ? 0 : due.size();
      // Permissões que sobraram (nada vencido, ou menos que a capacidade) voltam ao semáforo.
      permissoes.release(capacidade - reivindicadas);
      if (reivindicadas == 0) {
        break;
      }
      for (Delivery d : due) {
        // A permissão já foi adquirida acima, em nome desta tarefa; ela devolve ao terminar.
        emVoo.add(CompletableFuture.runAsync(() -> comPermissaoJaAdquirida(() -> attempt(d)), entregas));
      }
      tentadas += reivindicadas;
    }
    // Espera o que ainda está em voo: o scheduler é fixedDelay e o ciclo seguinte reivindicaria
    // com estas ainda em andamento — a concorrência real deixaria de ser a que o teto declara.
    CompletableFuture.allOf(emVoo.toArray(CompletableFuture[]::new)).join();
    return tentadas;
  }

  /**
   * O semáforo é o teto de entregas simultâneas — e, por consequência, o que protege o pool de
   * conexões e o parceiro de receber uma rajada.
   *
   * <p>Virtual thread não cria conexão de banco nem paciência no destino: sem este limite, o lote
   * inteiro (100) sairia de uma vez sobre um pool de 5 conexões. <b>O limite é a feature</b> —
   * {@code newVirtualThreadPerTaskExecutor()} sozinho não tem nenhum.
   *
   * <p>A aquisição fica em {@link #retryDue()}, na thread do ciclo, porque é ela que precisa saber
   * quantas permissões há para dimensionar a reivindicação; a tarefa só devolve.
   */
  private void comPermissaoJaAdquirida(Runnable tarefa) {
    try {
      tarefa.run();
    } finally {
      permissoes.release();
    }
  }

  private void attempt(Delivery delivery) {
    // Segredo do endpoint, resolvido a cada tentativa: uma rotação entre a primeira tentativa e o
    // retry assina com o que vale agora, sem carregar o segredo antigo na linha da entrega.
    Optional<SigningMaterial> material = endpoints.resolveSigningMaterial(delivery.endpointId());
    if (material.isEmpty()) {
      delivery.markDead("endpoint desativado ou removido");
      gravarDesfecho(delivery);
      log.warn("Entrega {} encerrada: endpoint {} não está mais ativo", delivery.id(), delivery.endpointId());
      return;
    }
    // Instante da TENTATIVA, e o mesmo para as duas assinaturas: durante a rotacao o receptor
    // compara a que ele consegue calcular, e dois instantes diferentes fariam uma delas nao bater.
    Instant assinadoEm = Instant.now();
    String signature = signer.sign(delivery.payload(), material.get().secret(), assinadoEm);
    String previousSignature =
        material.get().hasPrevious()
            ? signer.sign(delivery.payload(), material.get().previousSecret(), assinadoEm)
            : null;
    WebhookSendResult result =
        client.send(
            new WebhookRequest(
                delivery.targetUrl(),
                delivery.payload(),
                delivery.eventId().toString(),
                delivery.eventType(),
                signature,
                previousSignature));

    if (result.success()) {
      delivery.markDelivered();
      log.info("Webhook do evento {} entregue ({})", delivery.eventId(), result.statusCode());
    } else {
      delivery.markFailed(result.detail(), properties.maxAttempts(), nextAttempt(delivery.attempts()));
      log.warn(
          "Falha ao entregar evento {} (tentativa {}): {}",
          delivery.eventId(),
          delivery.attempts() + 1,
          result.detail());
    }
    gravarDesfecho(delivery);
  }

  /**
   * O desfecho só vale se a posse ainda é desta tentativa. Quando não é, o lease venceu no meio
   * do POST e outra réplica já reivindicou: o resultado dela é o que fica, e este é descartado
   * com aviso — gravar por cima reabriria a duplicata que o lease existe para evitar.
   */
  private void gravarDesfecho(Delivery delivery) {
    if (!repository.saveOutcome(delivery)) {
      log.warn(
          "Desfecho da entrega {} descartado: a posse venceu e outro worker reivindicou (lease {} menor que o pior caso de uma tentativa?)",
          delivery.id(),
          lease);
    }
  }

  /**
   * Encerra o executor das entregas quando o contexto fecha (o Spring chama {@code close()} de bean
   * {@link AutoCloseable} sem configuração). Não espera o lote em voo: numa parada abrupta a posse
   * (lease) já cobre a entrega interrompida — ela volta a ser reivindicável quando o lease vence.
   */
  @Override
  public void close() {
    entregas.shutdownNow();
  }

  private Instant nextAttempt(int attempts) {
    long factor = 1L << Math.min(attempts, 6); // backoff exponencial, teto no 64x
    return Instant.now().plus(properties.baseBackoff().multipliedBy(factor));
  }
}
