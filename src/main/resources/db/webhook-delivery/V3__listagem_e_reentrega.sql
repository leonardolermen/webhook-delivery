-- V3: listagem por tenant e reentrega (gateway, plano "webhook deliveries", 2026-10-04).
-- A listagem do merchant ordena por created_at DESC, id DESC com cursor nos dois: duas entregas
-- criadas na mesma transação têm o mesmo created_at, e um cursor só por data pularia ou repetiria.
CREATE INDEX idx_deliveries_tenant_listagem ON deliveries (tenant_id, created_at DESC, id DESC);
CREATE INDEX idx_deliveries_tenant_status ON deliveries (tenant_id, status);

-- Reentrega manual guarda o que a entrega era antes de voltar a PENDING: o merchant que pede a
-- reentrega quer saber por que ela morreu, e last_error é sobrescrito pela próxima tentativa.
ALTER TABLE deliveries ADD COLUMN redelivered_at TIMESTAMPTZ;
ALTER TABLE deliveries ADD COLUMN last_error_before_redelivery VARCHAR(500);
