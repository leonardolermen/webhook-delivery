package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.client.HmacSigner;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.intake.DeliveryIntake;
import com.barrier.webhookdelivery.intake.DeliveryRequest;
import com.barrier.webhookdelivery.intake.IntakeResult;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Fluxo fim-a-fim sem Kafka: accept() registra, o scheduler entrega, o destino recebe assinado.
 * Requer Docker.
 */
@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class, properties = {
    "webhook-delivery.retry-delay-ms=200",
    "webhook-delivery.headers.prefix=X-Test"
})
@Testcontainers
class WebhookDeliveryIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  record Recebido(String body, Map<String, String> headers) {}
  static final Map<String, List<Recebido>> porCaminho = new ConcurrentHashMap<>();
  static HttpServer server;

  @BeforeAll
  static void sobe() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    for (String caminho : List.of("/a", "/b")) {
      server.createContext(caminho, ex -> {
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> h = new ConcurrentHashMap<>();
        ex.getRequestHeaders().forEach((k, v) -> h.put(k.toLowerCase(), v.getFirst()));
        porCaminho.computeIfAbsent(caminho, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(new Recebido(body, h));
        ex.sendResponseHeaders(200, -1);
        ex.close();
      });
    }
    server.start();
  }

  @AfterAll static void desce() { server.stop(0); }

  @Autowired DeliveryIntake intake;
  @Autowired WebhookEndpointService endpoints;
  @Autowired HmacSigner signer;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
    jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints");
    porCaminho.clear();
  }

  private String url(String caminho) { return "http://localhost:" + server.getAddress().getPort() + caminho; }

  @Test
  void entregaAssinadaAoEndpointInscritoENaoAoOutro() {
    WebhookEndpoint a = endpoints.register("t1", url("/a"), List.of("payment.*"));
    endpoints.register("t1", url("/b"), List.of("refund.*"));
    UUID eventId = UUID.randomUUID();

    IntakeResult r = intake.accept(new DeliveryRequest("t1", "payment.completed", eventId, "pay_1", "pay_1", "{\"id\":\"pay_1\"}", "corr-1"));

    assertThat(r).isEqualTo(new IntakeResult(1, 1));
    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(porCaminho.getOrDefault("/a", List.of())).hasSize(1));
    Recebido rec = porCaminho.get("/a").getFirst();
    assertThat(rec.body()).isEqualTo("{\"id\":\"pay_1\"}");
    assertThat(rec.headers()).containsEntry("x-test-event-id", eventId.toString()).containsEntry("x-test-event-type", "payment.completed");
    String assinatura = rec.headers().get("x-test-signature");
    long t = Long.parseLong(assinatura.substring(2, assinatura.indexOf(',')));
    assertThat(assinatura).isEqualTo(signer.sign(rec.body(), a.secret(), java.time.Instant.ofEpochSecond(t)));
    assertThat(porCaminho).doesNotContainKey("/b");
  }

  @Test
  void mesmoEventoDuasVezesEntregaUmaVezPorEndpoint() {
    endpoints.register("t1", url("/a"), List.of("*"));
    endpoints.register("t1", url("/b"), List.of("*"));
    UUID eventId = UUID.randomUUID();
    DeliveryRequest req = new DeliveryRequest("t1", "payment.completed", eventId, "pay_2", null, "{}", null);

    assertThat(intake.accept(req)).isEqualTo(new IntakeResult(2, 2));
    assertThat(intake.accept(req)).isEqualTo(new IntakeResult(2, 0));

    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      assertThat(porCaminho.getOrDefault("/a", List.of())).hasSize(1);
      assertThat(porCaminho.getOrDefault("/b", List.of())).hasSize(1);
    });
  }

  @Test
  void tenantSemEndpointNaoRegistraNada() {
    IntakeResult r = intake.accept(new DeliveryRequest("ninguem", "x.y", UUID.randomUUID(), "a", null, "{}", null));
    assertThat(r).isEqualTo(IntakeResult.NONE);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM webhook_delivery.deliveries", Integer.class)).isZero();
  }

  @Test
  void endpointDesativadoAposRegistroMataAEntrega() {
    WebhookEndpoint a = endpoints.register("t1", url("/a"), List.of("*"));
    jdbc.update("UPDATE webhook_delivery.webhook_endpoints SET active = false WHERE id = ?", a.id());
    // Registrado antes da desativação: simula a corrida real com uma linha já gravada.
    jdbc.update("""
        INSERT INTO webhook_delivery.deliveries (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload, status, attempts, next_attempt_at, created_at)
        VALUES (?, ?, ?, 'x', 'a', 't1', ?, '{}', 'PENDING', 0, now(), now())
        """, UUID.randomUUID(), UUID.randomUUID(), a.id(), url("/a"));

    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        assertThat(jdbc.queryForObject("SELECT status FROM webhook_delivery.deliveries", String.class)).isEqualTo("DEAD"));
    assertThat(porCaminho).doesNotContainKey("/a");
  }
}
