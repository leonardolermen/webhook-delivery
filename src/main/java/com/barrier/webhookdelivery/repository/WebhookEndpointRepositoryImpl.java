package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;

// @Repository mantido apesar do bean vir de @Import na autoconfig: ver o comentário em
// DeliveryRepositoryImpl sobre a tradução de exceção.
@Repository
public class WebhookEndpointRepositoryImpl implements WebhookEndpointRepository {

  /** Namespace do lock por tenant; arbitrário e estável, ver {@code WebhookEndpointJpaRepository#travarTenant}. */
  private static final int NAMESPACE_TRAVA_TENANT = 2_207;

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

  @Override
  public void lockTenant(String tenantId) {
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(
          "lockTenant precisa rodar dentro de uma transação: o advisory lock de transação é "
              + "liberado imediatamente sem ela.");
    }
    jpa.travarTenant(NAMESPACE_TRAVA_TENANT, tenantId);
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
