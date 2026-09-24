package com.barrier.webhookdelivery.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HttpWebhookClientTest {

  static HttpServer server;
  static final Map<String, String> headers = new ConcurrentHashMap<>();
  static volatile int statusToReturn = 200;

  @BeforeAll
  static void sobe() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/hook", ex -> {
      headers.clear();
      ex.getRequestHeaders().forEach((k, v) -> headers.put(k, v.getFirst()));
      ex.getRequestBody().readAllBytes();
      ex.sendResponseHeaders(statusToReturn, -1);
      ex.close();
    });
    server.start();
  }

  @AfterAll
  static void desce() { server.stop(0); }

  private static HttpWebhookClient cliente() {
    return cliente(true);
  }

  private static HttpWebhookClient cliente(boolean allowPrivateTargets) {
    return new HttpWebhookClient(new WebhookDeliveryProperties(
        0, null, 0, 0, null, Duration.ofSeconds(1), Duration.ofSeconds(1), allowPrivateTargets, 0, null,
        new WebhookDeliveryProperties.Headers("X-Gateway"), null, null, null));
  }

  private String url() { return "http://localhost:" + server.getAddress().getPort() + "/hook"; }

  @Test
  void enviaOsQuatroHeadersComOPrefixoConfigurado() {
    statusToReturn = 200;
    WebhookSendResult r = cliente().send(new WebhookRequest(url(), "{}", "evt-1", "payment.completed", "t=1,v1=abc", "t=1,v1=old"));
    assertThat(r.success()).isTrue();
    assertThat(headers).containsEntry("X-gateway-signature", "t=1,v1=abc")
        .containsEntry("X-gateway-signature-previous", "t=1,v1=old")
        .containsEntry("X-gateway-event-id", "evt-1")
        .containsEntry("X-gateway-event-type", "payment.completed");
  }

  @Test
  void naoDoisXxViraFalhaSemLancar() {
    statusToReturn = 500;
    WebhookSendResult r = cliente().send(new WebhookRequest(url(), "{}", "evt-2", "x", "s", null));
    assertThat(r.success()).isFalse();
    assertThat(r.statusCode()).isEqualTo(500);
  }

  @Test
  void destinoForaDoArViraFalhaSemLancar() {
    WebhookSendResult r = cliente().send(new WebhookRequest("http://localhost:1/hook", "{}", "evt-3", "x", "s", null));
    assertThat(r.success()).isFalse();
    assertThat(r.statusCode()).isZero();
  }

  /**
   * A política de destino vale também na hora do POST, não só no registro: entre um e outro o DNS
   * do parceiro pode passar a apontar para dentro (rebinding), e a URL gravada na entrega já
   * passou pela validação de registro há muito tempo.
   */
  @Test
  void modoEstritoRecusaDestinoInternoAntesDeConectar() {
    headers.clear();
    WebhookSendResult r = cliente(false).send(new WebhookRequest(url(), "{}", "evt-4", "x", "s", null));
    assertThat(r.success()).isFalse();
    assertThat(r.statusCode()).isZero();
    assertThat(r.detail()).contains("rede interna");
    assertThat(headers).as("o POST nao deveria ter chegado ao servidor").isEmpty();
  }
}
