package com.barrier.webhookdelivery.service;

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
class WebhookEndpointServiceIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired WebhookEndpointService service;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach void limpa() { jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints"); }

  /** O contrato do Barrier: PUT /v1/webhook-endpoints/{tenant} duas vezes = mesmo endpoint, mesmo segredo, URL nova. */
  @Test
  void registerSingleFazUpsertPreservandoSegredo() {
    WebhookEndpoint primeiro = service.registerSingle("t1", "https://acme/v1");
    WebhookEndpoint segundo = service.registerSingle("t1", "https://acme/v2");
    assertThat(segundo.id()).isEqualTo(primeiro.id());
    assertThat(segundo.secret()).isEqualTo(primeiro.secret());
    assertThat(segundo.targetUrl()).isEqualTo("https://acme/v2");
    assertThat(segundo.events()).isEqualTo(WebhookEndpoint.ALL_EVENTS);
    assertThat(service.listByTenant("t1")).hasSize(1);
  }

  @Test
  void rotacaoEDesativacaoPorId() {
    WebhookEndpoint e = service.register("t1", "https://acme/hook", List.of("*"));
    WebhookEndpoint r = service.rotateSecret(e.id()).orElseThrow();
    assertThat(r.secret()).isNotEqualTo(e.secret());
    assertThat(service.resolveSigningMaterial(e.id()).orElseThrow().previousSecret()).isEqualTo(e.secret());
    service.deactivate(e.id());
    assertThat(service.resolveSigningMaterial(e.id())).isEmpty();
  }
}
