package com.barrier.webhookdelivery.domain;

import java.time.Instant;
import java.util.UUID;

/** Posição de paginação: a última linha vista, na ordem created_at DESC, id DESC. */
public record DeliveryCursor(Instant createdAt, UUID id) {}
