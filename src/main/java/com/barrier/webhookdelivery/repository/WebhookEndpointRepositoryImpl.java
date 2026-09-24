package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;

// @Repository mantido apesar do bean vir de @Import na autoconfig: ver o comentário em
// DeliveryRepositoryImpl sobre a tradução de exceção.
@Repository
public class WebhookEndpointRepositoryImpl implements WebhookEndpointRepository {

  private final WebhookEndpointJpaRepository jpa;

  public WebhookEndpointRepositoryImpl(WebhookEndpointJpaRepository jpa) {
    this.jpa = jpa;
  }

  @Override
  public WebhookEndpoint save(WebhookEndpoint endpoint) {
    WebhookEndpointEntity entity = jpa.findById(endpoint.id()).orElseGet(WebhookEndpointEntity::new);
    entity.setId(endpoint.id());
    entity.setTenantId(endpoint.tenantId());
    entity.setTargetUrl(endpoint.targetUrl());
    entity.setSecret(endpoint.secret());
    entity.setPreviousSecret(endpoint.previousSecret());
    entity.setPreviousSecretUntil(endpoint.previousSecretUntil());
    entity.setEvents(endpoint.events());
    entity.setActive(endpoint.active());
    entity.setCreatedAt(entity.getCreatedAt() == null ? endpoint.createdAt() : entity.getCreatedAt());
    entity.setUpdatedAt(endpoint.updatedAt());
    return toDomain(jpa.save(entity));
  }

  @Override public Optional<WebhookEndpoint> findById(UUID id) { return jpa.findById(id).map(WebhookEndpointRepositoryImpl::toDomain); }
  @Override public List<WebhookEndpoint> findByTenantId(String tenantId) { return jpa.findByTenantIdOrderByCreatedAtAsc(tenantId).stream().map(WebhookEndpointRepositoryImpl::toDomain).toList(); }
  @Override public List<WebhookEndpoint> findActiveByTenantId(String tenantId) { return jpa.findByTenantIdAndActiveTrueOrderByCreatedAtAsc(tenantId).stream().map(WebhookEndpointRepositoryImpl::toDomain).toList(); }
  @Override public List<WebhookEndpoint> findAll() { return jpa.findAll().stream().map(WebhookEndpointRepositoryImpl::toDomain).toList(); }

  private static WebhookEndpoint toDomain(WebhookEndpointEntity e) {
    return new WebhookEndpoint(e.getId(), e.getTenantId(), e.getTargetUrl(), e.getSecret(),
        e.getPreviousSecret(), e.getPreviousSecretUntil(), e.getEvents(), e.isActive(),
        e.getCreatedAt(), e.getUpdatedAt());
  }
}
