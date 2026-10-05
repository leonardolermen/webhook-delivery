package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DeliveryQueryTest {
  @Test
  void limiteZeroViraVinte() {
    DeliveryQuery query = new DeliveryQuery(null, null, null, null, null, 0);
    assertThat(query.limit()).isEqualTo(20);
  }

  @Test
  void limiteAcimaDeCemEhRecusado() {
    assertThatThrownBy(() -> new DeliveryQuery(null, null, null, null, null, 101))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
  }
}
