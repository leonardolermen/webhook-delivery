package com.barrier.webhookdelivery.repository;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Mapeamento JPA do endpoint de callback de um tenant.
 *
 * <p>Sem {@code @ToString}: imprimiria o segredo de HMAC em log.
 */
@Entity
@Table(name = "webhook_endpoints", schema = "webhook_delivery")
@Getter(AccessLevel.PACKAGE)
@Setter(AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class WebhookEndpointEntity {

  @Id
  @Column(name = "id", nullable = false)
  private java.util.UUID id;

  @Column(name = "tenant_id", nullable = false, length = 40)
  private String tenantId;

  @Column(name = "target_url", nullable = false, length = 500)
  private String targetUrl;

  /**
   * Em texto: assinar exige o valor. Diferente das API keys dos tenants, que ficam como hash —
   * lá basta comparar. Criptografia em repouso é item da Fase 6 e vale para esta coluna.
   */
  @Column(name = "secret", length = 120)
  private String secret;

  @Column(name = "previous_secret", length = 120)
  private String previousSecret;

  @Column(name = "previous_secret_until")
  private Instant previousSecretUntil;

  /** text[] nativo do Postgres; Hibernate 6.1+ mapeia List<String> sem conversor. */
  @JdbcTypeCode(SqlTypes.ARRAY)
  @Column(name = "events", nullable = false, columnDefinition = "text[]")
  private List<String> events;

  @Column(name = "active", nullable = false)
  private boolean active;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  /**
   * Controle otimista: o {@code save} copia todos os campos sobre a entidade lida; sem isto uma
   * gravação baseada em leitura antiga desfazia rotação de segredo ou desativação, em silêncio.
   * Gerido pelo Hibernate — o domínio não o conhece.
   */
  @Version
  @Column(name = "version", nullable = false)
  private long version;

}
