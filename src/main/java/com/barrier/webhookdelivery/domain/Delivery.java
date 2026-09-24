package com.barrier.webhookdelivery.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Entrega de um webhook para o cliente. Objeto de domínio (sem JPA); a persistência é feita
 * por um adapter no pacote {@code repository}.
 */
public class Delivery {

  /**
   * Tamanho da coluna {@code last_error}. O detalhe de uma falha vem de fora (mensagem de
   * exceção com a URL inteira, corpo de erro do parceiro) e não tem limite; gravá-lo inteiro
   * estourava a coluna, o save falhava e contador, backoff e transição para DEAD se perdiam — a
   * entrega voltava do lease com o contador antigo, para sempre.
   */
  public static final int MAX_ERROR_LENGTH = 500;

  private final UUID id;
  private final UUID eventId;
  private final UUID endpointId;
  private final String eventType;
  private final String aggregateId;
  private final String tenantId;
  private final String targetUrl;
  private final String payload;

  /**
   * Chave de ordenação da entrega: ordem <b>estrita</b> por chave. Uma entrega só sai depois que
   * todas as mais antigas da mesma chave chegaram a estado terminal (DELIVERED ou DEAD) —
   * inclusive enquanto uma predecessora espera o backoff de uma falha.
   *
   * <p>No Barrier é o subject, no gateway é o payment — nunca o tenant (serializaria o parceiro
   * grande) nem o assessment (a decisão e a mudança de nível do mesmo cliente têm assessments
   * diferentes e precisam ser ordenadas entre si). {@code null} significa "sem ordem exigida" —
   * fail-open, o desconhecido não trava a fila.
   */
  private final String partitionKey;

  private DeliveryStatus status;
  private int attempts;
  private String lastError;
  private Instant nextAttemptAt;
  private Instant claimedAt;

  /**
   * Token de posse da tentativa em curso. Nasce na reivindicação e é a credencial para gravar o
   * desfecho: o repositório só aceita o resultado de quem apresenta o token vigente. Um worker
   * cujo lease venceu (e cuja entrega outro reivindicou) ainda carrega o token antigo, e a
   * gravação dele é recusada em vez de sobrescrever o resultado mais novo.
   */
  private UUID claimToken;
  private final Instant createdAt;
  private Instant deliveredAt;

  private Delivery(
      UUID id,
      UUID eventId,
      UUID endpointId,
      String eventType,
      String aggregateId,
      String tenantId,
      String targetUrl,
      String payload,
      String partitionKey,
      Instant createdAt) {
    this.id = id;
    this.eventId = eventId;
    this.endpointId = endpointId;
    this.eventType = eventType;
    this.aggregateId = aggregateId;
    this.tenantId = tenantId;
    this.targetUrl = targetUrl;
    this.payload = payload;
    this.partitionKey = partitionKey;
    this.status = DeliveryStatus.PENDING;
    this.attempts = 0;
    this.nextAttemptAt = createdAt;
    // Nasce LIVRE, e isto inverte uma regra anterior.
    //
    // Antes: nascia reivindicada (claimedAt = createdAt) porque quem criava era quem tentava a
    // entrega em seguida — a linha já nasce vencida (next_attempt_at = created_at), e o scheduler
    // a pegava enquanto o POST original ainda estava em andamento, dobrando a entrega numa
    // instância só (migration V003).
    //
    // Agora o listener não entrega mais: quem entrega é sempre o pool, pelo retryDue(). Não há com
    // quem competir, e nascer presa deixaria toda PRIMEIRA entrega parada até o lease de 2 minutos
    // vencer.
    this.claimedAt = null;
    this.createdAt = createdAt;
  }

  /** Cria uma entrega pendente para o evento. */
  public static Delivery create(
      UUID eventId,
      UUID endpointId,
      String eventType,
      String aggregateId,
      String tenantId,
      String targetUrl,
      String payload,
      String partitionKey) {
    return new Delivery(
        UUID.randomUUID(),
        eventId,
        endpointId,
        eventType,
        aggregateId,
        tenantId,
        targetUrl,
        payload,
        partitionKey,
        Instant.now());
  }

  /** Reconstrói a partir da persistência. */
  public static Delivery rehydrate(
      UUID id,
      UUID eventId,
      UUID endpointId,
      String eventType,
      String aggregateId,
      String tenantId,
      String targetUrl,
      String payload,
      String partitionKey,
      DeliveryStatus status,
      int attempts,
      String lastError,
      Instant nextAttemptAt,
      Instant claimedAt,
      UUID claimToken,
      Instant createdAt,
      Instant deliveredAt) {
    Delivery d =
        new Delivery(
            id,
            eventId,
            endpointId,
            eventType,
            aggregateId,
            tenantId,
            targetUrl,
            payload,
            partitionKey,
            createdAt);
    d.status = status;
    d.attempts = attempts;
    d.lastError = lastError;
    d.nextAttemptAt = nextAttemptAt;
    d.claimedAt = claimedAt;
    d.claimToken = claimToken;
    d.deliveredAt = deliveredAt;
    return d;
  }

  /** Toma posse para uma tentativa. Chamado pelo repositório, dentro da transação de reivindicação. */
  public void claim(Instant now, UUID token) {
    this.claimedAt = now;
    this.claimToken = token;
  }

  /** Marca como entregue com sucesso. */
  public void markDelivered() {
    this.attempts++;
    this.status = DeliveryStatus.DELIVERED;
    this.lastError = null;
    this.nextAttemptAt = null;
    this.claimedAt = null;
    this.deliveredAt = Instant.now();
  }

  /**
   * Registra falha: reagenda se ainda há tentativas, senão marca como morta.
   *
   * <p>Libera a posse em qualquer caso: quem governa a próxima tentativa é o {@code nextAttemptAt}
   * (backoff exponencial), não a lease — que existe só para devolver à fila uma entrega cuja
   * instância morreu no meio do POST.
   */
  public void markFailed(String error, int maxAttempts, Instant nextAttemptAt) {
    this.attempts++;
    this.lastError = truncate(error);
    this.claimedAt = null;
    if (this.attempts >= maxAttempts) {
      this.status = DeliveryStatus.DEAD;
      this.nextAttemptAt = null;
    } else {
      this.status = DeliveryStatus.FAILED;
      this.nextAttemptAt = nextAttemptAt;
    }
  }

  /**
   * Encerra sem retentativa. Existe para o endpoint que foi desativado entre a criação da entrega e
   * a tentativa: reagendar consumiria as tentativas todas entregando para um destino que o próprio
   * tenant desligou.
   */
  public void markDead(String error) {
    this.status = DeliveryStatus.DEAD;
    this.lastError = truncate(error);
    this.claimedAt = null;
    this.nextAttemptAt = null;
  }

  private static String truncate(String error) {
    return error == null || error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
  }

  public UUID id() {
    return id;
  }

  public UUID eventId() {
    return eventId;
  }

  public UUID endpointId() {
    return endpointId;
  }

  public String eventType() {
    return eventType;
  }

  public String aggregateId() {
    return aggregateId;
  }

  public String tenantId() {
    return tenantId;
  }

  public String targetUrl() {
    return targetUrl;
  }

  public String payload() {
    return payload;
  }

  public String partitionKey() {
    return partitionKey;
  }

  public DeliveryStatus status() {
    return status;
  }

  public int attempts() {
    return attempts;
  }

  public String lastError() {
    return lastError;
  }

  public Instant nextAttemptAt() {
    return nextAttemptAt;
  }

  public Instant claimedAt() {
    return claimedAt;
  }

  public UUID claimToken() {
    return claimToken;
  }

  public Instant createdAt() {
    return createdAt;
  }

  public Instant deliveredAt() {
    return deliveredAt;
  }
}
