package com.barrier.webhookdelivery.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class, properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class WebhookEndpointRepositoryIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired WebhookEndpointRepository repository;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints");
  }

  @Test
  void persisteEventsComoArrayEFiltraAtivosPorTenant() {
    WebhookEndpoint a = repository.save(WebhookEndpoint.register("t1", "https://a/hook", List.of("payment.*")));
    WebhookEndpoint b = repository.save(WebhookEndpoint.register("t1", "https://b/hook", List.of("*")));
    repository.save(WebhookEndpoint.register("t2", "https://c/hook", List.of("*")));
    repository.save(b.deactivate());

    assertThat(repository.findById(a.id())).get().extracting(WebhookEndpoint::events)
        .isEqualTo(List.of("payment.*"));
    assertThat(repository.findByTenantId("t1")).hasSize(2);
    assertThat(repository.findActiveByTenantId("t1")).extracting(WebhookEndpoint::id).containsExactly(a.id());
  }

  /** O schema da lib e o historico Flyway proprio existem: e a autoconfig que os cria. */
  @Test
  void flywayDaLibRodouNoSchemaProprio() {
    Integer versoes = jdbc.queryForObject(
        "SELECT count(*) FROM webhook_delivery.flyway_schema_history_webhook_delivery WHERE success", Integer.class);
    assertThat(versoes).isGreaterThanOrEqualTo(1);
  }
}
