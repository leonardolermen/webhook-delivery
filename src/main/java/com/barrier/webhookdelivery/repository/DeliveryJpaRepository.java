package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.DeliveryStatus;
import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import jakarta.persistence.QueryHint;

interface DeliveryJpaRepository extends JpaRepository<DeliveryEntity, UUID> {

  boolean existsByEventId(UUID eventId);

  /**
   * Desfecho condicionado à posse: só grava se {@code claim_token} ainda é o da reivindicação
   * que fez a tentativa, e zera a posse na mesma escrita. Zero linhas afetadas = token vencido,
   * outro worker é o dono agora. JPQL pelo mesmo motivo de {@code selectClaimable}.
   */
  @Modifying
  @Query(
      """
      UPDATE DeliveryEntity d
         SET d.status = :status,
             d.attempts = :attempts,
             d.lastError = :lastError,
             d.nextAttemptAt = :nextAttemptAt,
             d.deliveredAt = :deliveredAt,
             d.claimedAt = NULL,
             d.claimToken = NULL
       WHERE d.id = :id
         AND d.claimToken = :claimToken
      """)
  int gravarDesfecho(
      @Param("id") UUID id,
      @Param("claimToken") UUID claimToken,
      @Param("status") DeliveryStatus status,
      @Param("attempts") int attempts,
      @Param("lastError") String lastError,
      @Param("nextAttemptAt") Instant nextAttemptAt,
      @Param("deliveredAt") Instant deliveredAt);

  /**
   * Trava a REIVINDICAÇÃO de entregas no cluster inteiro, por transação.
   *
   * <p>Fecha a última janela da ordem por chave de partição, e ela só era alcançável com réplicas
   * de verdade. {@code selectClaimable} tem {@code PESSIMISTIC_WRITE} + {@code SKIP LOCKED}, então
   * dois pods nunca reivindicam a <b>mesma</b> linha; mas a trava de ordem é um {@code NOT EXISTS}
   * sobre as <b>outras</b> linhas da mesma chave, e {@code SKIP LOCKED} não pula linha distinta.
   * Pod A tranca a entrega 1 do subject X e ainda não commitou; sob {@code READ COMMITTED} o
   * {@code claimed_at} de A é invisível para B, que enxerga "ninguém em voo" e reivindica a entrega
   * 2 do mesmo X. As duas saem em paralelo, fora de ordem — exatamente a garantia que a chave de
   * partição existe para dar. O filtro em memória do lote resolve isso dentro de um pod e não tem
   * como resolver entre pods.
   *
   * <p><b>Serializa o claim, não a entrega.</b> O que fica sob a trava é um {@code SELECT} mais os
   * {@code UPDATE}s de posse — milissegundos, sem rede. Os POSTs continuam paralelos e fora da
   * transação, que é onde o tempo do pipeline realmente está.
   *
   * <p><b>{@code try} e não a variante que espera</b>: com cinco pods e ciclo de 1s, quem não pega
   * a trava simplesmente pula o ciclo e tenta de novo — sem fila de espera, sem deadlock e sem
   * empilhar transações abertas atrás de uma lenta. Perder um ciclo custa 1 segundo de latência;
   * esperar custa disponibilidade.
   *
   * <p><b>Por que advisory aqui e tabela no {@code SingletonJobLock}</b>: lá o lease dura os
   * minutos de um download e precisa sobreviver à morte do pod, o que descarta um lock ligado à
   * sessão. Aqui é o oposto — a exclusão vale só enquanto a transação curta existe, e "morreu o
   * pod, soltou o lock" é justamente o comportamento desejado. A variante {@code _xact_} é a que
   * casa com isso, porque libera no commit ou no rollback sem ninguém precisar lembrar.
   *
   * <p>SQL nativo é inevitável (não há JPQL para isto) e, ao contrário das consultas de tabela, é
   * seguro: {@code pg_try_advisory_xact_lock} vive em {@code pg_catalog}, que está sempre no
   * {@code search_path} — o problema de schema que motivou o JPQL acima não se aplica.
   */
  @Query(value = "SELECT pg_try_advisory_xact_lock(:chave)", nativeQuery = true)
  boolean tentarTravarReivindicacao(@Param("chave") long chave);

  /**
   * Entregas vencidas cuja posse está livre ou expirada, travadas para reivindicação exclusiva.
   *
   * <p><b>Ordem estrita por chave de partição.</b> Uma entrega fica bloqueada enquanto existir
   * outra da mesma chave que seja (a) mais antiga e ainda não terminal — inclusive em backoff,
   * sem posse — ou (b) em voo, com posse ativa. Antes só (b) valia: a predecessora falhava,
   * soltava a posse, e a sucessora saía na frente durante o backoff. Terminal (DELIVERED/DEAD)
   * libera a chave: ordem não vira bloqueio eterno num parceiro fora do ar. O desempate por
   * {@code createdAt} na ordenação faz a escolha dentro da chave ser estável.
   *
   * <p><b>Cap por endpoint.</b> A subconsulta conta as entregas do mesmo endpoint com posse ativa
   * (reivindicadas por este ou outro pod, ainda dentro do lease) e exclui o endpoint quando chega
   * ao teto. Cobre o que já está em voo; o que entra no MESMO lote é limitado em memória em
   * {@code DeliveryRepositoryImpl.claimDue}, porque as posses deste lote ainda não foram gravadas
   * quando a consulta roda.
   *
   * <p>JPQL e não SQL nativo de propósito: a tabela vive no schema {@code webhook_delivery} e o
   * {@code hibernate.default_schema} não se aplica a consultas nativas — uma delas quebraria em
   * runtime dependendo do {@code search_path} da conexão.
   *
   * <p>{@code jakarta.persistence.lock.timeout = -2} é o valor que o Hibernate interpreta como
   * {@code SKIP LOCKED} ({@code LockOptions.SKIP_LOCKED}). Sem ele, o {@code PESSIMISTIC_WRITE}
   * faria as réplicas <b>esperarem</b> umas às outras em vez de pegarem conjuntos disjuntos, o que
   * troca a entrega duplicada por uma fila serializada.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
  @Query(
      """
      SELECT d FROM DeliveryEntity d
       WHERE d.status IN :statuses
         AND d.nextAttemptAt <= :now
         AND (d.claimedAt IS NULL OR d.claimedAt < :leaseCutoff)
         AND (SELECT COUNT(v) FROM DeliveryEntity v
               WHERE v.endpointId = d.endpointId
                 AND v.status IN :statuses
                 AND v.claimedAt IS NOT NULL
                 AND v.claimedAt >= :leaseCutoff) < :maxPerEndpoint
         AND (d.partitionKey IS NULL
              OR NOT EXISTS (SELECT 1 FROM DeliveryEntity irma
                              WHERE irma.partitionKey = d.partitionKey
                                AND irma.id <> d.id
                                AND irma.status IN :statuses
                                AND (irma.createdAt < d.createdAt
                                     OR (irma.claimedAt IS NOT NULL
                                         AND irma.claimedAt >= :leaseCutoff))))
       ORDER BY d.nextAttemptAt ASC, d.createdAt ASC
      """)
  List<DeliveryEntity> selectClaimable(
      @Param("statuses") List<DeliveryStatus> statuses,
      @Param("now") Instant now,
      @Param("leaseCutoff") Instant leaseCutoff,
      @Param("maxPerEndpoint") long maxPerEndpoint,
      Limit limit);
}
