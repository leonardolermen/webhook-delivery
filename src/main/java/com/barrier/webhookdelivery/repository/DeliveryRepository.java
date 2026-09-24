package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.Delivery;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Repositório de domínio das entregas. */
public interface DeliveryRepository {

  /**
   * Grava o desfecho de uma tentativa (DELIVERED, FAILED ou DEAD) <b>se</b> a entrega ainda
   * pertence a quem a reivindicou: a escrita é condicionada ao {@code claimToken} da entrega e
   * zera a posse. Retorna {@code false} quando o token não é mais o vigente — o lease venceu e
   * outra réplica reivindicou; o resultado desta tentativa é descartado, e não sobrescreve o
   * mais novo.
   */
  boolean saveOutcome(Delivery delivery);

  /**
   * Insere a entrega se ainda não existe uma para o mesmo {@code (eventId, endpointId)}.
   *
   * @return {@code true} se inseriu, {@code false} se já existia — sem exceção nos dois casos, e
   *     sem marcar para rollback a transação de quem chamou
   */
  boolean saveIfAbsent(Delivery delivery);

  boolean existsByEventId(UUID eventId);

  Optional<Delivery> findById(UUID id);

  /**
   * Reivindica entregas prontas para (re)tentativa — PENDING ou FAILED com {@code nextAttemptAt}
   * vencido — marcando posse por {@code lease}.
   *
   * <p>{@code maxPerEndpoint} limita quantas entregas do mesmo endpoint ficam em voo ao mesmo
   * tempo, contando as já reivindicadas (posse ativa) e as deste lote: um destino fora do ar não
   * monopoliza os workers.
   *
   * <p>Substitui o antigo {@code findDue}, que só lia: sem posse, réplicas concorrentes postavam a
   * mesma entrega e o cliente recebia o veredito de KYC duplicado.
   */
  List<Delivery> claimDue(Instant now, int limit, Duration lease, int maxPerEndpoint);
}
