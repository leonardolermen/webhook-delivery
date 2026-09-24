package com.barrier.webhookdelivery.repository;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface WebhookEndpointJpaRepository extends JpaRepository<WebhookEndpointEntity, UUID> {
  List<WebhookEndpointEntity> findByTenantIdOrderByCreatedAtAsc(String tenantId);
  List<WebhookEndpointEntity> findByTenantIdAndActiveTrueOrderByCreatedAtAsc(String tenantId);
}
