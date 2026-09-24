package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SigningMaterialTest {

  @Test
  void toStringNaoExpoeOsSegredos() {
    SigningMaterial m = new SigningMaterial("segredo-atual-123", "segredo-anterior-456");
    assertThat(m.toString())
        .doesNotContain("segredo-atual-123")
        .doesNotContain("segredo-anterior-456")
        .contains("secret=***", "previousSecret=***");
  }

  @Test
  void semSegredoAnteriorMostraNull() {
    assertThat(SigningMaterial.of("s3cr3t").toString()).doesNotContain("s3cr3t").contains("previousSecret=null");
  }
}
