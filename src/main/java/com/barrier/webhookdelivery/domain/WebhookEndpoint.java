package com.barrier.webhookdelivery.domain;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Endereço de callback de um tenant e o segredo que assina as entregas dele.
 *
 * <p>São as duas metades do mesmo problema: o endereço faz o resultado do KYC chegar ao cliente
 * certo, e o segredo próprio faz só o Barrier conseguir provar que aquele resultado é dele. Com
 * segredo compartilhado, quem conhecesse o de um tenant forjaria callbacks para todos.
 *
 * @param previousSecret segredo anterior, válido até {@code previousSecretUntil}; {@code null} fora
 *     de uma rotação
 */
public record WebhookEndpoint(
    UUID id,
    String tenantId,
    String targetUrl,
    String secret,
    String previousSecret,
    Instant previousSecretUntil,
    List<String> events,
    boolean active,
    Instant createdAt,
    Instant updatedAt) {

  private static final Set<String> ALLOWED_SCHEMES = Set.of("http", "https");
  private static final Set<String> LOCAL_HOSTS = Set.of("localhost", "127.0.0.1", "[::1]");
  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int SECRET_BYTES = 32;
  public static final List<String> ALL_EVENTS = List.of("*");

  public static WebhookEndpoint register(String tenantId, String targetUrl, List<String> events) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId obrigatório");
    }
    validate(targetUrl);
    Instant now = Instant.now();
    return new WebhookEndpoint(
        UUID.randomUUID(), tenantId, targetUrl.trim(), newSecret(), null, null,
        normalize(events), true, now, now);
  }

  /** Atualização de URL <b>preserva o segredo</b>: trocar de quebra derrubaria a verificação do cliente sem ninguém ter pedido rotação. */
  public WebhookEndpoint withTargetUrl(String newTargetUrl) {
    validate(newTargetUrl);
    return new WebhookEndpoint(id, tenantId, newTargetUrl.trim(), secret, previousSecret,
        previousSecretUntil, events, active, createdAt, Instant.now());
  }

  public WebhookEndpoint withEvents(List<String> newEvents) {
    return new WebhookEndpoint(id, tenantId, targetUrl, secret, previousSecret,
        previousSecretUntil, normalize(newEvents), active, createdAt, Instant.now());
  }

  public boolean subscribedTo(String eventType) {
    return EventTypeMatcher.matches(events, eventType);
  }

  /** Lista vazia ou nula significa "tudo": o caso do Barrier, que tem um endpoint por tenant e nunca filtrou. */
  private static List<String> normalize(List<String> events) {
    if (events == null || events.isEmpty()) {
      return ALL_EVENTS;
    }
    return List.copyOf(events);
  }

  /**
   * Gera um segredo novo e mantém o atual válido por {@code overlap}.
   *
   * <p>A janela é o que torna a rotação uma operação sem downtime: durante ela a entrega vai
   * assinada pelos dois, e o cliente troca a chave quando puder. Sem sobreposição, rotacionar
   * obrigaria a combinar um instante exato com cada parceiro — e na prática ninguém rotaciona.
   */
  public WebhookEndpoint rotateSecret(Duration overlap) {
    return new WebhookEndpoint(
        id,
        tenantId,
        targetUrl,
        newSecret(),
        secret,
        secret == null ? null : Instant.now().plus(overlap),
        events,
        active,
        createdAt,
        Instant.now());
  }

  public WebhookEndpoint deactivate() {
    return new WebhookEndpoint(
        id,
        tenantId,
        targetUrl,
        secret,
        previousSecret,
        previousSecretUntil,
        events,
        false,
        createdAt,
        Instant.now());
  }

  /** Segredo anterior enquanto a janela de rotação vale; {@code null} fora dela. */
  public String usablePreviousSecret() {
    if (previousSecret == null || previousSecretUntil == null) {
      return null;
    }
    return Instant.now().isBefore(previousSecretUntil) ? previousSecret : null;
  }

  private static String newSecret() {
    byte[] bytes = new byte[SECRET_BYTES];
    RANDOM.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  /**
   * Recusa destino que não seja HTTP(S) absoluto e exige TLS fora de host local: o payload leva
   * documento, nome e o veredito de PLD-FT do cliente final, e em texto claro qualquer ponto do
   * caminho lê tudo — a assinatura HMAC prova origem, não confidencialidade.
   */
  private static void validate(String targetUrl) {
    if (targetUrl == null || targetUrl.isBlank()) {
      throw new IllegalArgumentException("targetUrl obrigatório");
    }
    URI uri;
    try {
      uri = new URI(targetUrl.trim());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("targetUrl inválida: " + targetUrl, e);
    }
    if (uri.getScheme() == null || !ALLOWED_SCHEMES.contains(uri.getScheme().toLowerCase())) {
      throw new IllegalArgumentException("targetUrl deve ser http ou https: " + targetUrl);
    }
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("targetUrl sem host: " + targetUrl);
    }
    if ("http".equalsIgnoreCase(uri.getScheme()) && !LOCAL_HOSTS.contains(uri.getHost())) {
      throw new IllegalArgumentException(
          "targetUrl sem TLS: " + targetUrl + " — http só é aceito para host local (dev)");
    }
  }
}
