-- Estado consolidado das V001–V008 do services/webhook-api do Barrier, já na forma da lib.
--
-- O que mudou de lá para cá, e por quê:
--   assessment_id → aggregate_id : a lib não sabe o que é uma avaliação.
--   endpoint_id                  : um tenant pode ter N endpoints; a idempotência passa a ser
--                                   (event_id, endpoint_id) — o mesmo evento vai uma vez a cada um.
--   event_type                   : o que os endpoints filtram; também sai em header.
--   webhook_endpoints.id/events  : PK deixa de ser o tenant. events = '{*}' é "tudo", o caso do
--                                   Barrier, que nunca filtrou.
--   sem job_locks                : é lease de jobs do Barrier, não da entrega.
--
-- Vive no schema webhook_delivery, próprio da lib, com histórico Flyway próprio: dois produtos com
-- histórias de migração diferentes não conseguem compartilhar uma sequência de versões.

CREATE TABLE webhook_endpoints (
    id                    UUID         PRIMARY KEY,
    tenant_id             VARCHAR(40)  NOT NULL,
    target_url            VARCHAR(500) NOT NULL,
    -- Em texto: assinar exige o valor. Criptografia em repouso é responsabilidade do consumidor.
    secret                VARCHAR(120),
    previous_secret       VARCHAR(120),
    previous_secret_until TIMESTAMPTZ,
    events                TEXT[]       NOT NULL DEFAULT '{*}',
    -- active em vez de DELETE: desligar a entrega é reversível e auditável.
    active                BOOLEAN      NOT NULL DEFAULT true,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_webhook_endpoints_tenant_active ON webhook_endpoints (tenant_id, active);

CREATE TABLE deliveries (
    id              UUID         PRIMARY KEY,
    event_id        UUID         NOT NULL,
    endpoint_id     UUID         NOT NULL,
    event_type      VARCHAR(120) NOT NULL,
    aggregate_id    VARCHAR(64)  NOT NULL,
    tenant_id       VARCHAR(40)  NOT NULL,
    -- Copiada do endpoint na criação: a URL pode mudar entre tentativas e a entrega segue a que
    -- valia quando o evento nasceu (comportamento herdado do Barrier).
    target_url      VARCHAR(500) NOT NULL,
    -- TEXT e não JSONB: a normalização do JSONB alteraria os bytes assinados.
    payload         TEXT         NOT NULL,
    -- Chave de ordenação. Entregas com a mesma chave nunca correm em paralelo. NULL = sem ordem.
    partition_key   VARCHAR(64),
    status          VARCHAR(20)  NOT NULL,
    attempts        INTEGER      NOT NULL DEFAULT 0,
    last_error      VARCHAR(500),
    next_attempt_at TIMESTAMPTZ,
    -- Posse por um worker, com expiração (lease). Ver DeliveryRepositoryImpl.claimDue.
    claimed_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL,
    delivered_at    TIMESTAMPTZ,
    CONSTRAINT uq_deliveries_event_endpoint UNIQUE (event_id, endpoint_id)
);

CREATE INDEX idx_deliveries_claimable ON deliveries (status, next_attempt_at, claimed_at);
CREATE INDEX idx_deliveries_tenant ON deliveries (tenant_id);
CREATE INDEX idx_deliveries_event ON deliveries (event_id);
CREATE INDEX idx_deliveries_partition_key_em_voo
    ON deliveries (partition_key, claimed_at)
    WHERE status IN ('PENDING', 'FAILED') AND partition_key IS NOT NULL;
