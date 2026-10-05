package com.barrier.webhookdelivery.domain;

import java.time.Instant;

/** Filtros da listagem por tenant. Todo campo é opcional menos o limite, que tem teto. */
public record DeliveryQuery(
    DeliveryStatus status,
    String eventType,
    String aggregateId,
    Instant since,
    DeliveryCursor after,
    int limit) {
  public static final int DEFAULT_LIMIT = 20;
  public static final int MAX_LIMIT = 100;

  public DeliveryQuery {
    if (limit == 0) {
      limit = DEFAULT_LIMIT;
    }
    if (limit < 1 || limit > MAX_LIMIT) {
      throw new IllegalArgumentException("limit deve ficar entre 1 e " + MAX_LIMIT);
    }
  }
}
