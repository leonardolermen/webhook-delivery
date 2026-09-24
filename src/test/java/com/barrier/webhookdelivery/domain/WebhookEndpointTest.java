package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class WebhookEndpointTest {

  @Test
  void registroNasceAtivoComIdESegredo() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of("payment.*"));
    assertThat(e.id()).isNotNull();
    assertThat(e.secret()).isNotBlank();
    assertThat(e.active()).isTrue();
    assertThat(e.subscribedTo("payment.completed")).isTrue();
    assertThat(e.subscribedTo("refund.completed")).isFalse();
  }

  @Test
  void eventsVazioViraTodos() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of());
    assertThat(e.events()).isEqualTo(WebhookEndpoint.ALL_EVENTS);
  }

  @Test
  void trocarUrlPreservaSegredoEId() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of("*"));
    WebhookEndpoint f = e.withTargetUrl("https://acme/v2/webhook");
    assertThat(f.id()).isEqualTo(e.id());
    assertThat(f.secret()).isEqualTo(e.secret());
    assertThat(f.targetUrl()).isEqualTo("https://acme/v2/webhook");
  }

  @Test
  void rotacaoMantemAnteriorPelaJanela() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of("*"));
    WebhookEndpoint r = e.rotateSecret(Duration.ofHours(1));
    assertThat(r.secret()).isNotEqualTo(e.secret());
    assertThat(r.usablePreviousSecret()).isEqualTo(e.secret());
  }

  @Test
  void httpForaDeLocalhostEhRecusado() {
    assertThatThrownBy(() -> WebhookEndpoint.register("t1", "http://acme/webhook", List.of("*")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sem TLS");
  }
}
