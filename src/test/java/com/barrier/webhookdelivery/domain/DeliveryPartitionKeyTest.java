package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A entrega precisa carregar a chave que decide o que pode correr em paralelo com o quê.
 *
 * <p>A chave é escolhida por quem chama, e não é o tenant nem o agregado: serializar por tenant
 * limitaria o parceiro grande a uma entrega por vez, e por agregado não ordenaria nada quando dois
 * eventos sobre a mesma entidade de negócio nascem de agregados diferentes (no Barrier, a decisão e
 * a mudança de nível de risco do mesmo cliente — por isso lá a chave é o subject).
 */
class DeliveryPartitionKeyTest {

  @Test
  void guardaAChaveDeParticao() {
    Delivery d =
        Delivery.create(
            UUID.randomUUID(), UUID.randomUUID(), "assessment.completed", "a-1", "default", "http://localhost:9000", "{}", "subject-42");

    assertThat(d.partitionKey()).isEqualTo("subject-42");
  }

  /**
   * Sem subject no payload a entrega não exige ordem — e não pode travar por isso. Fail-open: o
   * desconhecido não bloqueia a fila.
   */
  @Test
  void chaveNulaEhPermitida() {
    Delivery d =
        Delivery.create(
            UUID.randomUUID(), UUID.randomUUID(), "assessment.completed", "a-1", "default", "http://localhost:9000", "{}", null);

    assertThat(d.partitionKey()).isNull();
  }
}
