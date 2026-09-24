package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.Delivery;

final class DeliveryEntityMapper {

  private DeliveryEntityMapper() {}

  static DeliveryEntity toEntity(Delivery d) {
    DeliveryEntity e = new DeliveryEntity();
    e.setId(d.id());
    e.setEventId(d.eventId());
    e.setEndpointId(d.endpointId());
    e.setEventType(d.eventType());
    e.setAggregateId(d.aggregateId());
    e.setTenantId(d.tenantId());
    e.setTargetUrl(d.targetUrl());
    e.setPayload(d.payload());
    e.setPartitionKey(d.partitionKey());
    e.setStatus(d.status());
    e.setAttempts(d.attempts());
    e.setLastError(d.lastError());
    e.setNextAttemptAt(d.nextAttemptAt());
    e.setClaimedAt(d.claimedAt());
    e.setClaimToken(d.claimToken());
    e.setCreatedAt(d.createdAt());
    e.setDeliveredAt(d.deliveredAt());
    return e;
  }

  static Delivery toDomain(DeliveryEntity e) {
    return Delivery.rehydrate(
        e.getId(),
        e.getEventId(),
        e.getEndpointId(),
        e.getEventType(),
        e.getAggregateId(),
        e.getTenantId(),
        e.getTargetUrl(),
        e.getPayload(),
        e.getPartitionKey(),
        e.getStatus(),
        e.getAttempts(),
        e.getLastError(),
        e.getNextAttemptAt(),
        e.getClaimedAt(),
        e.getClaimToken(),
        e.getCreatedAt(),
        e.getDeliveredAt());
  }
}
