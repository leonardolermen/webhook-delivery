package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.DeliveryStatus;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

// @Repository mantido apesar de o bean ser registrado por @Import na autoconfig, e não por
// component scan: é ele que instala a tradução de exceção do PersistenceExceptionTranslationPostProcessor
// (InvalidDataAccessApiUsageException em vez de IllegalStateException cru), e
// recusaReivindicarForaDeTransacao depende disso.
@Repository
public class DeliveryRepositoryImpl implements DeliveryRepository {

  /**
   * Identificador da trava de reivindicação. Valor arbitrário e <b>estável</b>: o que importa é que
   * todas as réplicas usem o mesmo número, e que ele não colida com outro advisory lock desta base
   * — hoje não há nenhum outro ({@code SingletonJobLock} é lease em tabela, de propósito).
   */
  private static final long TRAVA_REIVINDICACAO = 8_314_027L;

  private final DeliveryJpaRepository jpa;

  @PersistenceContext private EntityManager em;

  public DeliveryRepositoryImpl(DeliveryJpaRepository jpa) {
    this.jpa = jpa;
  }

  /**
   * {@code @Transactional} (REQUIRED) porque o UPDATE em JPQL exige transação; quem chama (o
   * worker, fora da transação de reivindicação) não tem uma, e ela é curta: um UPDATE.
   */
  @Override
  @Transactional
  public boolean saveOutcome(Delivery delivery) {
    if (delivery.claimToken() == null) {
      throw new IllegalArgumentException("entrega sem token de posse: o desfecho só pode vir de quem reivindicou");
    }
    int afetadas =
        jpa.gravarDesfecho(
            delivery.id(),
            delivery.claimToken(),
            delivery.status(),
            delivery.attempts(),
            delivery.lastError(),
            delivery.nextAttemptAt(),
            delivery.deliveredAt());
    return afetadas == 1;
  }

  /**
   * {@code ON CONFLICT DO NOTHING} e não {@code save} + {@code catch (DataIntegrityViolationException)}:
   * o catch só funcionava porque cada {@code save} commitava sozinho. Dentro do
   * {@code @Transactional} de quem chama, o INSERT fica para o flush, a violação estoura fora do
   * try, e no Postgres a transação inteira de quem chamou é abortada por uma duplicata que devia ser
   * um no-op.
   *
   * <p>SQL nativo é seguro aqui, ao contrário do caso descrito em
   * {@code DeliveryJpaRepository.selectClaimable}: a tabela vai qualificada com o schema, que é
   * fixo ({@code webhook_delivery}), então o {@code search_path} da conexão não muda o alvo.
   *
   * <p>{@code @Transactional} (REQUIRED) porque {@code executeUpdate} exige transação: entra na de
   * quem chama quando existe, e abre uma curta quando não.
   */
  @Override
  @Transactional
  public boolean saveIfAbsent(Delivery delivery) {
    DeliveryEntity e = DeliveryEntityMapper.toEntity(delivery);
    int inseridas =
        em.createNativeQuery(
                """
                INSERT INTO webhook_delivery.deliveries
                  (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                   partition_key, status, attempts, last_error, next_attempt_at, claimed_at, claim_token,
                   created_at, delivered_at)
                VALUES
                  (:id, :eventId, :endpointId, :eventType, :aggregateId, :tenantId, :targetUrl, :payload,
                   :partitionKey, :status, :attempts, :lastError, :nextAttemptAt, :claimedAt, :claimToken,
                   :createdAt, :deliveredAt)
                ON CONFLICT (event_id, endpoint_id) DO NOTHING
                """)
            .setParameter("id", e.getId())
            .setParameter("eventId", e.getEventId())
            .setParameter("endpointId", e.getEndpointId())
            .setParameter("eventType", e.getEventType())
            .setParameter("aggregateId", e.getAggregateId())
            .setParameter("tenantId", e.getTenantId())
            .setParameter("targetUrl", e.getTargetUrl())
            .setParameter("payload", e.getPayload())
            .setParameter("partitionKey", e.getPartitionKey())
            .setParameter("status", e.getStatus().name())
            .setParameter("attempts", e.getAttempts())
            .setParameter("lastError", e.getLastError())
            .setParameter("nextAttemptAt", e.getNextAttemptAt())
            .setParameter("claimedAt", e.getClaimedAt())
            .setParameter("claimToken", e.getClaimToken())
            .setParameter("createdAt", e.getCreatedAt())
            .setParameter("deliveredAt", e.getDeliveredAt())
            .executeUpdate();
    return inseridas == 1;
  }

  @Override
  public boolean existsByEventId(UUID eventId) {
    return jpa.existsByEventId(eventId);
  }

  @Override
  public Optional<Delivery> findById(UUID id) {
    return jpa.findById(id).map(DeliveryEntityMapper::toDomain);
  }

  /**
   * A posse é gravada por dirty checking: as entidades vêm gerenciadas da consulta com lock, e o
   * {@code claimed_at} é persistido no commit da transação que envolve esta chamada — que é curta e
   * <b>não</b> contém o POST para o cliente.
   *
   * <p>São <b>três</b> travas, e cada uma cobre um alcance que as outras não cobrem: o {@code SKIP
   * LOCKED} da consulta impede dois pods na mesma linha, o filtro em memória impede duas linhas da
   * mesma chave no mesmo lote, e a trava de reivindicação impede dois pods em linhas diferentes da
   * mesma chave. Tirar qualquer uma reabre um caso que os testes das outras duas não pegam.
   */
  @Override
  public List<Delivery> claimDue(Instant now, int limit, Duration lease, int maxPerEndpoint) {
    // Sem transação, pg_try_advisory_xact_lock auto-commita e o lock morre no mesmo instante em que
    // nasce: a proteção some sem nenhum sinal, que é o modo de falha que esta frente inteira existe
    // para eliminar. Melhor recusar alto do que reivindicar achando que está protegido.
    if (!TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(
          "claimDue precisa rodar dentro de uma transação: a exclusão entre réplicas depende de um "
              + "advisory lock de transação, que sem ela é liberado imediatamente.");
    }
    if (!jpa.tentarTravarReivindicacao(TRAVA_REIVINDICACAO)) {
      // Outra réplica está reivindicando agora. Pular o ciclo custa um intervalo do scheduler;
      // esperar na fila prenderia esta transação atrás da lentidão da outra.
      return List.of();
    }
    List<DeliveryEntity> claimable =
        jpa.selectClaimable(
            List.of(DeliveryStatus.PENDING, DeliveryStatus.FAILED),
            now,
            now.minus(lease),
            maxPerEndpoint,
            Limit.of(limit));
    // SEGUNDA trava, e ela é indispensável: a query exclui chaves JÁ em voo, mas duas entregas
    // recém-criadas do mesmo subject ainda não têm claimedAt — nenhuma bloqueia a outra, e as duas
    // seriam elegíveis no mesmo lote. Sem esta linha a ordem quebraria dentro de um único ciclo,
    // que é justamente o caso mais comum: os dois eventos do mesmo cliente chegam juntos.
    Set<String> chavesNoLote = new HashSet<>();
    // Mesmo raciocínio para o cap por endpoint: a consulta contou as posses JÁ gravadas; as deste
    // lote ainda não existem no banco, então o teto dentro do lote é aplicado aqui.
    Map<UUID, Integer> porEndpointNoLote = new HashMap<>();
    List<DeliveryEntity> lote =
        claimable.stream()
            .filter(e -> e.getPartitionKey() == null || chavesNoLote.add(e.getPartitionKey()))
            .filter(e -> porEndpointNoLote.merge(e.getEndpointId(), 1, Integer::sum) <= maxPerEndpoint)
            .toList();

    // Posse = instante (para o lease) + token (para o desfecho): o token é o que impede um worker
    // com lease vencido de gravar por cima de quem reivindicou depois dele.
    lote.forEach(
        entity -> {
          entity.setClaimedAt(now);
          entity.setClaimToken(UUID.randomUUID());
        });
    return lote.stream().map(DeliveryEntityMapper::toDomain).toList();
  }
}
