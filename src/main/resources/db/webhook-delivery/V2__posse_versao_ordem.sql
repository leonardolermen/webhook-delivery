-- Controle otimista do endpoint. O save copia todos os campos da cópia lida; sem versão, uma
-- atualização de URL baseada em leitura antiga regravava o segredo anterior a uma rotação, ou
-- reativava um endpoint recém-desativado. Com @Version o UPDATE leva WHERE version = lida e a
-- segunda gravação falha alto em vez de sobrescrever em silêncio.
ALTER TABLE webhook_endpoints ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

-- Ordem estrita por chave: a consulta de reivindicação agora bloqueia a sucessora enquanto houver
-- irmã mais antiga não terminal (não só a em voo). O NOT EXISTS passa a olhar created_at.
DROP INDEX IF EXISTS idx_deliveries_partition_key_em_voo;
CREATE INDEX idx_deliveries_partition_key_pendente
    ON deliveries (partition_key, created_at, claimed_at)
    WHERE status IN ('PENDING', 'FAILED') AND partition_key IS NOT NULL;

-- Token de posse da tentativa. claimed_at diz ATÉ QUANDO a posse vale; o token diz DE QUEM ela é.
-- O desfecho (DELIVERED/FAILED/DEAD) só grava com WHERE claim_token = o da reivindicação: um worker
-- cujo lease venceu não sobrescreve o resultado de quem reivindicou depois dele.
ALTER TABLE deliveries ADD COLUMN claim_token UUID;

-- Cap de entregas em voo por endpoint: a reivindicação conta as posses ativas do mesmo endpoint.
CREATE INDEX idx_deliveries_endpoint_em_voo
    ON deliveries (endpoint_id, claimed_at)
    WHERE status IN ('PENDING', 'FAILED') AND claimed_at IS NOT NULL;
