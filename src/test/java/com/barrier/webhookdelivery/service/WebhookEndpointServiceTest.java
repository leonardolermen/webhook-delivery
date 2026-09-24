package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.repository.WebhookEndpointRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** A política de destino (SSRF) vale nas três portas de entrada de URL: register, registerSingle e update. */
class WebhookEndpointServiceTest {

  /** Fake em memória: o que está em teste é a política, não a persistência. */
  static final class RepositorioEmMemoria implements WebhookEndpointRepository {
    final List<WebhookEndpoint> dados = new ArrayList<>();
    @Override public WebhookEndpoint save(WebhookEndpoint e) { dados.removeIf(x -> x.id().equals(e.id())); dados.add(e); return e; }
    @Override public Optional<WebhookEndpoint> findById(UUID id) { return dados.stream().filter(x -> x.id().equals(id)).findFirst(); }
    @Override public List<WebhookEndpoint> findByTenantId(String t) { return dados.stream().filter(x -> x.tenantId().equals(t)).toList(); }
    @Override public List<WebhookEndpoint> findActiveByTenantId(String t) { return findByTenantId(t).stream().filter(WebhookEndpoint::active).toList(); }
    @Override public List<WebhookEndpoint> findAll() { return List.copyOf(dados); }
    @Override public void lockTenant(String tenantId) {}
  }

  private static WebhookEndpointService servico(boolean allowPrivateTargets) {
    return new WebhookEndpointService(new RepositorioEmMemoria(), new WebhookDeliveryProperties(
        0, null, 0, 0, null, null, null, allowPrivateTargets, 0, null, null, null, null, null));
  }

  @Test
  void registerRecusaRedeInternaPorPadrao() {
    assertThatThrownBy(() -> servico(false).register("t1", "https://10.0.0.5/hook", List.of("*")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rede interna");
  }

  @Test
  void registerSingleRecusaRedeInternaPorPadrao() {
    assertThatThrownBy(() -> servico(false).registerSingle("t1", "https://192.168.0.10/hook"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rede interna");
  }

  @Test
  void updateRecusaRedeInternaPorPadrao() {
    WebhookEndpointService s = servico(false);
    WebhookEndpoint e = s.register("t1", "https://93.184.216.34/hook", List.of("*"));
    assertThatThrownBy(() -> s.update(e.id(), "https://127.0.0.1/hook", List.of("*")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rede interna");
    assertThat(s.find(e.id()).orElseThrow().targetUrl()).isEqualTo("https://93.184.216.34/hook");
  }

  @Test
  void comAllowPrivateTargetsAceitaLocalhost() {
    WebhookEndpoint e = servico(true).register("t1", "http://localhost:8080/hook", List.of("*"));
    assertThat(e.targetUrl()).isEqualTo("http://localhost:8080/hook");
  }
}
