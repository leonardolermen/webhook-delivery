package com.barrier.webhookdelivery.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.DeliveryCursor;
import com.barrier.webhookdelivery.domain.DeliveryQuery;
import com.barrier.webhookdelivery.domain.DeliveryStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Listagem do merchant: filtros opcionais e cursor de dois campos (created_at, id). */
@SpringBootTest(
    classes = com.barrier.webhookdelivery.testapp.TestApplication.class,
    properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class DeliveryListingIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired DeliveryRepository repository;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
  }

  @Test
  void listaSoDoTenantNaOrdemMaisRecentePrimeiro() {
    Instant base = Instant.parse("2026-10-04T12:00:00Z");
    UUID antiga = grava("t1", "payment.completed", "DELIVERED", base);
    UUID nova = grava("t1", "payment.failed", "DEAD", base.plusSeconds(60));
    grava("t2", "payment.completed", "DELIVERED", base.plusSeconds(120));

    List<Delivery> lista =
        repository.findByTenant("t1", new DeliveryQuery(null, null, null, null, null, 20));

    assertThat(lista).extracting(Delivery::id).containsExactly(nova, antiga);
  }

  @Test
  void cursorComMesmoCreatedAtNaoPulaNemRepete() {
    Instant mesmo = Instant.parse("2026-10-04T12:00:00Z");
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ids.add(grava("t1", "payment.completed", "DELIVERED", mesmo));
    }

    List<Delivery> primeira =
        repository.findByTenant("t1", new DeliveryQuery(null, null, null, null, null, 2));
    Delivery ultima = primeira.get(1);
    List<Delivery> segunda =
        repository.findByTenant(
            "t1",
            new DeliveryQuery(
                null, null, null, null, new DeliveryCursor(ultima.createdAt(), ultima.id()), 2));
    Delivery ultima2 = segunda.get(1);
    List<Delivery> terceira =
        repository.findByTenant(
            "t1",
            new DeliveryQuery(
                null, null, null, null, new DeliveryCursor(ultima2.createdAt(), ultima2.id()), 2));

    List<UUID> vistos =
        Stream.of(primeira, segunda, terceira)
            .flatMap(List::stream)
            .map(Delivery::id)
            .toList();
    assertThat(vistos).hasSize(5).doesNotHaveDuplicates().containsExactlyInAnyOrderElementsOf(ids);
  }

  @Test
  void filtraPorStatusTipoAgregadoEDesde() {
    Instant base = Instant.parse("2026-10-04T12:00:00Z");
    UUID alvo = grava("t1", "payment.failed", "DEAD", base.plusSeconds(10));
    grava("t1", "payment.failed", "DELIVERED", base.plusSeconds(20));
    grava("t1", "payment.completed", "DEAD", base.plusSeconds(30));
    grava("t1", "payment.failed", "DEAD", base.minusSeconds(3600));

    List<Delivery> lista =
        repository.findByTenant(
            "t1", new DeliveryQuery(DeliveryStatus.DEAD, "payment.failed", "a-1", base, null, 20));

    assertThat(lista).extracting(Delivery::id).containsExactly(alvo);
  }

  @Test
  void porIdExigeOTenantCerto() {
    UUID id = grava("t1", "payment.completed", "DELIVERED", Instant.now());

    assertThat(repository.findByTenantAndId("t1", id)).isPresent();
    assertThat(repository.findByTenantAndId("t2", id)).isEmpty();
  }

  private UUID grava(String tenant, String eventType, String status, Instant createdAt) {
    UUID id = UUID.randomUUID();
    Timestamp timestamp = Timestamp.from(createdAt);
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
               (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                partition_key, status, attempts, next_attempt_at, created_at)
        VALUES (?, ?, ?, ?, 'a-1', ?, 'http://localhost:9000', '{}', NULL, ?, 0, ?, ?)
        """,
        id, UUID.randomUUID(), UUID.randomUUID(), eventType, tenant, status, timestamp, timestamp);
    return id;
  }
}
