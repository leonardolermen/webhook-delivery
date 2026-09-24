package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * SSRF: o tenant controla a URL de callback. Exigir TLS não impede que ele faça o servidor postar
 * (com documento e veredito do cliente final no corpo) num serviço interno alcançável. A política
 * olha o endereço efetivamente acessado — literal ou resolvido por DNS — e recusa rede local,
 * privada e link-local, salvo por configuração explícita de desenvolvimento.
 */
class TargetUrlPolicyTest {

  private static final TargetUrlPolicy ESTRITA = new TargetUrlPolicy(false);
  private static final TargetUrlPolicy DEV = new TargetUrlPolicy(true);

  @ParameterizedTest
  @ValueSource(strings = {
      "https://127.0.0.1/hook",
      "https://10.0.0.5/hook",
      "https://172.16.0.1/hook",
      "https://192.168.1.1/hook",
      "https://169.254.169.254/latest/meta-data",
      "https://0.0.0.0/hook",
      "https://[::1]/hook",
      "https://[fd00::1]/hook",
      "https://localhost/hook",
      "http://127.0.0.1:8080/hook"
  })
  void recusaDestinoEmRedeInternaQuandoEstrita(String url) {
    assertThatThrownBy(() -> ESTRITA.check(url))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rede interna");
  }

  @Test
  void recusaHostQueNaoResolve() {
    assertThatThrownBy(() -> ESTRITA.check("https://nao-existe.invalid/hook"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("não resolve");
  }

  @Test
  void aceitaEnderecoPublicoLiteral() {
    assertThatCode(() -> ESTRITA.check("https://93.184.216.34/hook")).doesNotThrowAnyException();
  }

  @Test
  void modoDevAceitaTudoSemResolver() {
    assertThatCode(() -> DEV.check("https://127.0.0.1/hook")).doesNotThrowAnyException();
    assertThatCode(() -> DEV.check("http://localhost:8080/hook")).doesNotThrowAnyException();
    assertThatCode(() -> DEV.check("https://nao-existe.invalid/hook")).doesNotThrowAnyException();
  }
}
