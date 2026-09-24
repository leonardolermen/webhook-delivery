package com.barrier.webhookdelivery.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuração da entrega. Os defaults são os que o Barrier rodava em produção — o valor de cada um
 * tem a conta no {@code application.yml} de lá (pool de conexões vs. workers, lease maior que o pior
 * caso de connect+read timeout).
 *
 * <p>{@code maxInFlightPerEndpoint} (padrão 4) é o teto de entregas simultâneas de um MESMO
 * endpoint: um parceiro fora do ar, com dezenas de entregas em retry, não ocupa todos os workers.
 *
 * <p>{@code allowPrivateTargets} (padrão {@code false}) desliga a política de destino que recusa
 * rede interna (SSRF) — só para desenvolvimento, com callback em {@code localhost}. Ver
 * {@code TargetUrlPolicy}.
 *
 * <p>Não há mais {@code target-url} nem {@code secret} globais: eram fallback de desenvolvimento e,
 * com dois tenants, entregavam o callback de um no endpoint do outro. Quem quiser um destino de dev
 * registra um endpoint na subida.
 */
@ConfigurationProperties(prefix = "webhook-delivery")
public record WebhookDeliveryProperties(
    int workers,
    Duration lease,
    long retryDelayMs,
    int maxAttempts,
    Duration baseBackoff,
    Duration connectTimeout,
    Duration readTimeout,
    boolean allowPrivateTargets,
    int maxInFlightPerEndpoint,
    Duration secretRotationOverlap,
    Headers headers,
    String correlationMdcKey,
    Scheduler scheduler,
    Flyway flyway) {

  public WebhookDeliveryProperties {
    if (workers <= 0) workers = 16;
    if (maxInFlightPerEndpoint <= 0) maxInFlightPerEndpoint = 4;
    if (lease == null) lease = Duration.ofMinutes(2);
    if (retryDelayMs <= 0) retryDelayMs = 5000;
    if (maxAttempts <= 0) maxAttempts = 5;
    if (baseBackoff == null) baseBackoff = Duration.ofSeconds(30);
    if (connectTimeout == null) connectTimeout = Duration.ofSeconds(2);
    if (readTimeout == null) readTimeout = Duration.ofSeconds(10);
    if (secretRotationOverlap == null) secretRotationOverlap = Duration.ofHours(24);
    if (headers == null) headers = new Headers(null);
    if (correlationMdcKey == null || correlationMdcKey.isBlank()) correlationMdcKey = "correlationId";
    if (scheduler == null) scheduler = new Scheduler(true);
    if (flyway == null) flyway = new Flyway(false, null);
  }

  /** Prefixo dos headers: o Barrier usa {@code X-Barrier}, o gateway {@code X-Gateway}. */
  public record Headers(String prefix) {
    public Headers {
      if (prefix == null || prefix.isBlank()) prefix = "X-Webhook";
    }

    public String signature() { return prefix + "-Signature"; }
    public String previousSignature() { return prefix + "-Signature-Previous"; }
    public String eventId() { return prefix + "-Event-Id"; }
    public String eventType() { return prefix + "-Event-Type"; }
  }

  public record Scheduler(boolean enabled) {}

  /**
   * @param baselineOnMigrate para o consumidor cujo schema já existe (o Barrier, depois da V009
   *     dele): marca a versão {@code baselineVersion} como aplicada em vez de rodar a V1
   */
  public record Flyway(boolean baselineOnMigrate, String baselineVersion) {
    public Flyway {
      if (baselineVersion == null || baselineVersion.isBlank()) baselineVersion = "1";
    }
  }
}
