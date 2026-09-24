package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.DeliveryStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Mapeamento JPA de uma entrega de webhook. */
@Entity
@Table(name = "deliveries", schema = "webhook_delivery")
@Getter(AccessLevel.PACKAGE)
@Setter(AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class DeliveryEntity {

  @Id
  @Column(name = "id", nullable = false)
  private UUID id;

  @Column(name = "event_id", nullable = false)
  private UUID eventId;

  @Column(name = "endpoint_id", nullable = false)
  private UUID endpointId;

  @Column(name = "event_type", nullable = false, length = 120)
  private String eventType;

  @Column(name = "aggregate_id", nullable = false, length = 64)
  private String aggregateId;

  @Column(name = "tenant_id", nullable = false, length = 40)
  private String tenantId;

  @Column(name = "target_url", nullable = false, length = 500)
  private String targetUrl;

  @Column(name = "payload", nullable = false, length = 4000)
  private String payload;

  /** Chave de ordenacao da entrega; NULL = sem ordem exigida. Ver V008. */
  @Column(name = "partition_key", length = 64)
  private String partitionKey;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 20)
  private DeliveryStatus status;

  @Column(name = "attempts", nullable = false)
  private int attempts;

  @Column(name = "last_error", length = 500)
  private String lastError;

  @Column(name = "next_attempt_at")
  private Instant nextAttemptAt;

  /** Posse desta entrega por um worker, com expiração. Ver migration V003. */
  @Column(name = "claimed_at")
  private Instant claimedAt;

  /** Credencial da posse; ver {@code Delivery#claimToken}. NULL quando ninguém tem a entrega. */
  @Column(name = "claim_token")
  private java.util.UUID claimToken;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "delivered_at")
  private Instant deliveredAt;

}
