package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.Delivery;
import com.barrier.webhookdelivery.domain.DeliveryStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Repository;
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

  public DeliveryRepositoryImpl(DeliveryJpaRepository jpa) {
    this.jpa = jpa;
  }

  @Override
  public Delivery save(Delivery delivery) {
    return DeliveryEntityMapper.toDomain(jpa.save(DeliveryEntityMapper.toEntity(delivery)));
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
  public List<Delivery> claimDue(Instant now, int limit, Duration lease) {
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
            Limit.of(limit));
    // SEGUNDA trava, e ela é indispensável: a query exclui chaves JÁ em voo, mas duas entregas
    // recém-criadas do mesmo subject ainda não têm claimedAt — nenhuma bloqueia a outra, e as duas
    // seriam elegíveis no mesmo lote. Sem esta linha a ordem quebraria dentro de um único ciclo,
    // que é justamente o caso mais comum: os dois eventos do mesmo cliente chegam juntos.
    Set<String> chavesNoLote = new HashSet<>();
    List<DeliveryEntity> lote =
        claimable.stream()
            .filter(e -> e.getPartitionKey() == null || chavesNoLote.add(e.getPartitionKey()))
            .toList();

    lote.forEach(entity -> entity.setClaimedAt(now));
    return lote.stream().map(DeliveryEntityMapper::toDomain).toList();
  }
}
