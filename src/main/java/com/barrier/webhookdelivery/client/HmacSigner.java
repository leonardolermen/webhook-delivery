package com.barrier.webhookdelivery.client;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Assina a entrega com HMAC-SHA256 sobre {@code <instante>.<corpo>}, no formato
 * {@code t=<epoch-segundos>,v1=<hex>}.
 *
 * <p><b>Por que o instante entra.</b> Assinar so o corpo torna a entrega replayavel para sempre:
 * quem capturou um callback de KYC uma vez pode reenvia-lo ao parceiro indefinidamente, com
 * assinatura valida, e o parceiro nao tem como distinguir do original. O {@code t=} permite ao
 * receptor recusar o que e velho demais.
 *
 * <p><b>Por que DENTRO da assinatura.</b> Se o instante viajasse em header proprio, fora do que se
 * assina, o atacante trocaria o valor por "agora" e o replay voltaria a passar na janela de
 * tolerancia — o controle pareceria existir sem verificar nada, o mesmo modo de falha que este
 * projeto ja fechou em outros lugares. O ponto entre instante e corpo evita que
 * {@code t=17} + {@code "00.x"} e {@code t=1700} + {@code ".x"} colidam.
 *
 * <p><b>Por que o instante e o da tentativa, nao o da criacao da entrega.</b> O
 * {@code WebhookDeliveryService} resolve segredo e assina a cada tentativa. Congelar o instante na
 * criacao faria toda retentativa posterior a janela de tolerancia do parceiro chegar velha e ser
 * recusada — a maquina de retry queimaria as tentativas todas entregando algo que o receptor foi
 * instruido a rejeitar.
 *
 * <p>O prefixo {@code v1=} existe para haver caminho de migracao: um esquema futuro entra como
 * {@code v2=} ao lado, e o receptor escolhe — mesmo desenho do
 * {@code -Signature-Previous} para rotacao de segredo.
 */
public class HmacSigner {

  private static final String ALGORITHM = "HmacSHA256";

  public String sign(String body, String secret, Instant instant) {
    long epochSeconds = instant.getEpochSecond();
    String assinado = epochSeconds + "." + body;
    return "t=" + epochSeconds + ",v1=" + hmacHex(assinado, secret);
  }

  private static String hmacHex(String conteudo, String secret) {
    try {
      Mac mac = Mac.getInstance(ALGORITHM);
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
      byte[] digest = mac.doFinal(conteudo.getBytes(StandardCharsets.UTF_8));
      return toHex(digest);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new IllegalStateException("Falha ao assinar webhook", e);
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16));
      sb.append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
