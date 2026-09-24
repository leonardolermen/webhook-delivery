package com.barrier.webhookdelivery.domain;

/**
 * Segredos com que uma entrega é assinada.
 *
 * @param secret segredo vigente do tenant
 * @param previousSecret segredo anterior, ainda dentro da janela de rotação; {@code null} fora dela.
 *     Quando presente, a entrega leva uma segunda assinatura, para o cliente que ainda não trocou a
 *     chave continuar verificando
 */
public record SigningMaterial(String secret, String previousSecret) {

  public static SigningMaterial of(String secret) {
    return new SigningMaterial(secret, null);
  }

  public boolean hasPrevious() {
    return previousSecret != null && !previousSecret.isBlank();
  }

  /** Mascarado pelo mesmo motivo de {@link WebhookEndpoint#toString()}: segredo em log é segredo vazado. */
  @Override
  public String toString() {
    return "SigningMaterial[secret=" + WebhookEndpoint.mascara(secret)
        + ", previousSecret=" + WebhookEndpoint.mascara(previousSecret) + "]";
  }
}
