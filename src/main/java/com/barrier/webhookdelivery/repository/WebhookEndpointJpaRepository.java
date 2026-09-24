package com.barrier.webhookdelivery.repository;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface WebhookEndpointJpaRepository extends JpaRepository<WebhookEndpointEntity, UUID> {

  /**
   * Serializa, por tenant e por transação, quem faz "lê e decide se insere" no endpoint único.
   *
   * <p>Chave em dois inteiros ({@code namespace}, {@code hashtext(tenant)}): esse espaço de chaves
   * é separado do de um inteiro de 64 bits que {@code DeliveryJpaRepository} usa para a
   * reivindicação, então os dois nunca colidem. A variante que espera (e não {@code try}) é a
   * certa aqui: a seção crítica é um SELECT e um INSERT, e a segunda chamada precisa enxergar o
   * resultado da primeira, não pular.
   */
  @Query(value = "SELECT pg_advisory_xact_lock(:namespace, hashtext(:tenantId))", nativeQuery = true)
  void travarTenant(@Param("namespace") int namespace, @Param("tenantId") String tenantId);

  List<WebhookEndpointEntity> findByTenantIdOrderByCreatedAtAsc(String tenantId);
  List<WebhookEndpointEntity> findByTenantIdAndActiveTrueOrderByCreatedAtAsc(String tenantId);
}
