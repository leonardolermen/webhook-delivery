package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Endpoints de callback registrados; N por tenant. */
public interface WebhookEndpointRepository {
  WebhookEndpoint save(WebhookEndpoint endpoint);
  Optional<WebhookEndpoint> findById(UUID id);
  /** Ordenado por criação: o mais antigo é o "endpoint único" que o Barrier conhece. */
  List<WebhookEndpoint> findByTenantId(String tenantId);
  List<WebhookEndpoint> findActiveByTenantId(String tenantId);
  List<WebhookEndpoint> findAll();
}
