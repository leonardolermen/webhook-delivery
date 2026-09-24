package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.Delivery;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Repositório de domínio das entregas. */
public interface DeliveryRepository {

  Delivery save(Delivery delivery);

  boolean existsByEventId(UUID eventId);

  Optional<Delivery> findById(UUID id);

  /**
   * Reivindica entregas prontas para (re)tentativa — PENDING ou FAILED com {@code nextAttemptAt}
   * vencido — marcando posse por {@code lease}.
   *
   * <p>Substitui o antigo {@code findDue}, que só lia: sem posse, réplicas concorrentes postavam a
   * mesma entrega e o cliente recebia o veredito de KYC duplicado.
   */
  List<Delivery> claimDue(Instant now, int limit, Duration lease);
}
