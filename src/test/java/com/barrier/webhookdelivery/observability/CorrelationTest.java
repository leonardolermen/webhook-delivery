package com.barrier.webhookdelivery.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class CorrelationTest {

  @Test
  void colocaNoMdcERestauraOAnterior() {
    MDC.put("cid", "antes");
    Correlation.run("cid", "durante", () -> assertThat(MDC.get("cid")).isEqualTo("durante"));
    assertThat(MDC.get("cid")).isEqualTo("antes");
    MDC.remove("cid");
  }

  @Test
  void semIdNaoTocaNoMdcELimpaAoFinal() {
    Correlation.run("cid", null, () -> assertThat(MDC.get("cid")).isNull());
    assertThat(MDC.get("cid")).isNull();
  }

  @Test
  void callRetornaOValorDoSupplierERestauraOMdc() {
    MDC.put("cid", "antes");
    String resultado = Correlation.call("cid", "durante", () -> {
      assertThat(MDC.get("cid")).isEqualTo("durante");
      return "valor-do-supplier";
    });
    assertThat(resultado).isEqualTo("valor-do-supplier");
    assertThat(MDC.get("cid")).isEqualTo("antes");
    MDC.remove("cid");
  }
}
