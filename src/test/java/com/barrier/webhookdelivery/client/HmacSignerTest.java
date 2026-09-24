package com.barrier.webhookdelivery.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class HmacSignerTest {

  private final HmacSigner signer = new HmacSigner();
  private static final Instant T = Instant.ofEpochSecond(1_700_000_000L);

  @Test
  void assinaturaEDeterministicaEDeclaraOInstante() {
    String a = signer.sign("{\"x\":1}", "secret", T);
    String b = signer.sign("{\"x\":1}", "secret", T);

    assertThat(a).isEqualTo(b).isEqualTo("t=1700000000,v1=" + hex(a));
  }

  @Test
  void segredosDiferentesGeramAssinaturasDiferentes() {
    assertThat(signer.sign("body", "s1", T)).isNotEqualTo(signer.sign("body", "s2", T));
  }

  /**
   * O instante entra no que e assinado, nao so no header.
   *
   * <p>E este o ponto do {@code t=}: se ele viajasse fora da assinatura, um atacante que capturou
   * uma entrega trocaria o valor por "agora" e o replay voltaria a passar na janela de tolerancia.
   * Assinar {@code <t>.<corpo>} amarra os dois — mexer no instante invalida a assinatura.
   */
  @Test
  void oInstanteEstaDENTRODoQueSeAssina() {
    String antes = signer.sign("body", "secret", T);
    String depois = signer.sign("body", "secret", T.plusSeconds(1));

    assertThat(hex(antes)).isNotEqualTo(hex(depois));
  }

  /**
   * Corpos diferentes no mesmo instante assinam diferente — guarda contra separador ambiguo.
   *
   * <p>Sem o ponto entre instante e corpo, {@code t=17} + corpo {@code "00.x"} e {@code t=1700} +
   * corpo {@code ".x"} produziriam a mesma entrada de HMAC.
   */
  @Test
  void oSeparadorNaoDeixaInstanteEcorpoSeConfundirem() {
    assertThat(signer.sign("00.x", "secret", Instant.ofEpochSecond(17)))
        .isNotEqualTo(signer.sign(".x", "secret", Instant.ofEpochSecond(1700)));
  }

  private static String hex(String assinatura) {
    return assinatura.substring(assinatura.indexOf("v1=") + 3);
  }
}
