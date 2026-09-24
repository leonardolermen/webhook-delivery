package com.barrier.webhookdelivery.service;

import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.barrier.webhookdelivery.domain.SigningMaterial;
import com.barrier.webhookdelivery.domain.TargetUrlPolicy;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.repository.WebhookEndpointRepository;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/** Registro dos endpoints de callback e resolução do destino/segredo de uma entrega. */
public class WebhookEndpointService {

  private static final Logger log = LoggerFactory.getLogger(WebhookEndpointService.class);

  private final WebhookEndpointRepository repository;
  private final Duration rotationOverlap;
  private final TargetUrlPolicy targetPolicy;

  public WebhookEndpointService(WebhookEndpointRepository repository, WebhookDeliveryProperties properties) {
    this.repository = repository;
    this.rotationOverlap = properties.secretRotationOverlap();
    this.targetPolicy = new TargetUrlPolicy(properties.allowPrivateTargets());
  }

  @Transactional
  public WebhookEndpoint register(String tenantId, String targetUrl, List<String> events) {
    targetPolicy.check(targetUrl);
    WebhookEndpoint salvo = repository.save(WebhookEndpoint.register(tenantId, targetUrl, events));
    log.info("Endpoint {} do tenant {} registrado (eventos {})", salvo.id(), tenantId, salvo.events());
    return salvo;
  }

  /**
   * Endpoint ÚNICO do tenant, o mais antigo: cria se não existe, senão atualiza a URL preservando o
   * segredo e forçando {@code events={"*"}}. É o contrato de {@code PUT /v1/webhook-endpoints/{tenantId}}
   * do Barrier, que não conhece id nem filtro; mantê-lo aqui deixa o controller de lá intocado.
   */
  @Transactional
  public WebhookEndpoint registerSingle(String tenantId, String targetUrl) {
    // Atômico por tenant: sem isto, duas chamadas simultâneas liam "vazio" e inseriam dois
    // endpoints "únicos", e o mesmo evento saía para os dois.
    targetPolicy.check(targetUrl);
    repository.lockTenant(tenantId);
    List<WebhookEndpoint> existentes = repository.findByTenantId(tenantId);
    if (existentes.isEmpty()) {
      return register(tenantId, targetUrl, WebhookEndpoint.ALL_EVENTS);
    }
    WebhookEndpoint atualizado = existentes.getFirst().withTargetUrl(targetUrl).withEvents(WebhookEndpoint.ALL_EVENTS);
    log.info("Endpoint único do tenant {} atualizado (segredo preservado)", tenantId);
    return repository.save(atualizado);
  }

  @Transactional
  public Optional<WebhookEndpoint> update(UUID id, String targetUrl, List<String> events) {
    targetPolicy.check(targetUrl);
    return repository.findById(id).map(e -> e.withTargetUrl(targetUrl).withEvents(events)).map(repository::save);
  }

  @Transactional
  public Optional<WebhookEndpoint> rotateSecret(UUID id) {
    Optional<WebhookEndpoint> rotacionado = repository.findById(id).map(e -> e.rotateSecret(rotationOverlap)).map(repository::save);
    rotacionado.ifPresent(e -> log.info("Segredo do endpoint {} rotacionado; o anterior vale até {}", id, e.previousSecretUntil()));
    return rotacionado;
  }

  @Transactional
  public Optional<WebhookEndpoint> deactivate(UUID id) {
    return repository.findById(id).map(WebhookEndpoint::deactivate).map(repository::save);
  }

  @Transactional(readOnly = true) public Optional<WebhookEndpoint> find(UUID id) { return repository.findById(id); }
  @Transactional(readOnly = true) public List<WebhookEndpoint> listByTenant(String tenantId) { return repository.findByTenantId(tenantId); }
  @Transactional(readOnly = true) public List<WebhookEndpoint> listAll() { return repository.findAll(); }

  /** Segredos com que a entrega deste endpoint deve ser assinada; vazio se ele sumiu ou foi desativado. */
  @Transactional(readOnly = true)
  public Optional<SigningMaterial> resolveSigningMaterial(UUID endpointId) {
    return repository.findById(endpointId)
        .filter(WebhookEndpoint::active)
        .filter(e -> e.secret() != null)
        .map(e -> new SigningMaterial(e.secret(), e.usablePreviousSecret()));
  }
}
