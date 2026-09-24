# webhook-delivery — extração do Barrier como biblioteca

Data: 2026-09-23. Status: aprovado em conversa, aguardando revisão do texto.

Biblioteca Maven que entrega webhooks assinados a endpoints de clientes, com
retry, ordenação por chave, idempotência e rotação de segredo. Nasce do
`services/webhook-api` do Barrier — não é reescrita, é mudança de endereço
com quatro generalizações. Primeiro consumidor: o Barrier. Segundo, e o que
justifica a extração: o Pix Gateway
(`pix-gateway/docs/superpowers/specs/2026-09-23-pix-gateway-design.md`, §7).

## 1. Por que biblioteca, em repo próprio, publicada por versão

- **Rejeitado — serviço compartilhado.** Obrigaria o gateway a ter Kafka e um
  segundo deploy, e acoplaria a operação de um KYC e um gateway de pagamento.
- **Rejeitado — copiar.** O próximo incidente seria corrigido em uma cópia só.
  As três travas do `claimDue` e o instante dentro da assinatura HMAC
  existem por causa de incidentes documentados; duas cópias divergem.
- **Rejeitado — módulo no monorepo do Barrier.** Era a recomendação inicial
  (refatoração num PR só, testes como rede). O dono preferiu repo próprio
  com versões publicadas desde o início: a lib tem ciclo de vida próprio e
  dois consumidores desde o dia 1, e um monorepo faria a versão do Barrier e
  a da lib andarem juntas sem motivo.
- **Custo de estar errado:** a refatoração cruza dois repos enquanto o
  Barrier se adapta. Mitigação: §6 — a lib nasce com os testes do Barrier e o
  Barrier só troca de dependência quando a suíte da lib está verde.

Coordenadas: `com.barrier:webhook-delivery`, pacote raiz
`com.barrier.webhookdelivery`. Publicação em **GitHub Packages** do repo
`leonardolermen/webhook-delivery`; consumidores autenticam com token de
leitura em `~/.m2/settings.xml`. Versionamento semântico; `0.x` enquanto a
API de entrada assenta. Toolchain igual ao Barrier: Java 25, Spring Boot 4,
Maven.

## 2. O que muda de lugar

**Vai para a lib** (pacotes de `com.barrier.webhook` → `com.barrier.webhookdelivery`):

| de | para | nota |
|---|---|---|
| `domain/*` | `domain/*` | `Delivery`, `DeliveryStatus`, `WebhookEndpoint`, `SigningMaterial` |
| `repository/*` | `repository/*` | entidades JPA, `claimDue` com as três travas |
| `service/WebhookDeliveryService`, `WebhookEndpointService`, `DeliveryRetryScheduler` | `service/*` | |
| `client/*` | `client/*` | `HmacSigner`, `HttpWebhookClient`, `WebhookClient`, records |
| `config/WebhookProperties`, `WorkerPoolConfig` | `config/*` | prefixo novo, §3.3 |
| `db/migration/V001–V008` | `db/migration/webhook-delivery/V1__*` | consolidadas numa migração inicial, §3.4 |
| testes: `WebhookDeliveryIntegrationTest` (reescrito sobre `DeliveryRequest`, sem Kafka), `DeliveryOrderingIntegrationTest`, `DeliveryClaimExclusionIntegrationTest`, `HmacSignerTest`, `DeliveryPartitionKeyTest`, `LayeredArchitectureTest` | `src/test` da lib | `WebhookLoadTest` fica no Barrier: mede Kafka → listener → POST |

**Fica no Barrier** (`services/webhook-api` continua um deployable):
`AssessmentCompletedListener` (Kafka), `DeliveryReconciliationJob` (relê o
*tópico*; é Barrier), `MalformedEventException`, `KafkaErrorHandlingConfig`,
controllers e DTOs, `AdminApiKeyFilter`, readiness guards, `OpenApiConfig`,
`JobLockConfig`, e os testes de controller/listener/API.

**A lib não depende de `commons`** (que puxa Kafka) nem de nada do Barrier.
Dependências: `spring-boot-starter-data-jpa`, `spring-web` (`RestClient`),
`flyway-core` + `flyway-database-postgresql`, `postgresql` (runtime). Teste:
Testcontainers, ArchUnit, WireMock (destino fake).

## 3. As quatro generalizações

### 3.1 Entrada

`WebhookDeliveryService.onEvent(EventEnvelope, tenantId)` vira:

```java
public interface DeliveryIntake {
  /** Registra a entrega para todo endpoint ativo do tenant inscrito no eventType. Idempotente por (eventId, endpointId). */
  IntakeResult accept(DeliveryRequest request);
}
public record DeliveryRequest(
    String tenantId, String eventType, UUID eventId, String aggregateId,
    String partitionKey, String payload, String correlationId) {}
public record IntakeResult(int deliveriesCreated, int endpointsMatched) {}
```

- `assessment_id` → `aggregate_id`. `partitionKey` vem pronto de quem chama
  (o Barrier extrai `subjectId` do payload como hoje; o gateway passa
  `payment_id`). `null` continua significando "sem ordem exigida".
- A lib ganha `Correlation` próprio (MDC; nome da chave em
  `webhook-delivery.correlation-mdc-key`, default `correlationId`), para não
  importar `commons`.
- `EventEnvelope` não entra na lib. O listener do Barrier faz
  `envelope → DeliveryRequest`.

### 3.2 N endpoints por tenant, com filtro por evento

- `webhook_endpoints`: `id UUID PK`, `tenant_id`, `target_url`, `secret`,
  `previous_secret`, `previous_secret_until`, `events TEXT[] NOT NULL`
  (`{"*"}` = todos), `active`, timestamps. Índice `(tenant_id, active)`.
- `deliveries`: ganha `endpoint_id UUID NOT NULL`; unicidade passa de
  `event_id` para `(event_id, endpoint_id)`. `target_url` continua copiado
  na linha (o endpoint pode mudar de URL entre tentativas — hoje é assim e
  fica assim).
- `accept` faz fan-out: uma `Delivery` por endpoint ativo cujo `events`
  contém `eventType` ou `*`. Match por igualdade e por prefixo com curinga
  final (`payment.*`) — só isso; sem glob geral.
- `WebhookEndpointService` ganha `register(tenantId, url, events)`,
  `update(id, …)`, `rotateSecret(id)`, `deactivate(id)`, `listByTenant`.
  **Mantém** `registerSingle(tenantId, url)`: upsert do endpoint único do
  tenant com `events={"*"}` — é o que o controller `PUT /v1/webhook-endpoints/{tenantId}`
  do Barrier passa a chamar, sem mudar contrato.
- `existsByEventId` (usado pela reconciliação do Barrier) continua: "existe
  alguma entrega deste evento".

### 3.3 Nomes configuráveis

- Prefixo `webhook-delivery.*`: `workers`, `lease`, `retry-delay-ms`,
  `max-attempts`, `base-backoff`, `connect-timeout`, `read-timeout`,
  `secret-rotation-overlap`, `headers.prefix`, `flyway.baseline-on-migrate`,
  `correlation-mdc-key`, `scheduler.enabled`.
- Headers: `${headers.prefix}-Signature`, `-Signature-Previous`, `-Event-Id`,
  e **novo** `-Event-Type`. Barrier configura `X-Barrier`; gateway,
  `X-Gateway`. O formato da assinatura (`t=…,v1=…`) não muda.
- `target-url` e `secret` globais (fallback de desenvolvimento) **saem** da
  lib. O Barrier, se quiser manter o fallback de dev, faz isso no próprio
  serviço registrando um endpoint na subida — não é responsabilidade da lib.
- Autoconfiguração Spring (`AutoConfiguration.imports`): o consumidor não
  precisa de `@ComponentScan` em pacote da lib. O Barrier já escaneia só
  `com.barrier.webhook`, de propósito; isso continua valendo.

### 3.4 Schema e Flyway próprios

- A lib é dona do schema `webhook_delivery` (fixo: as entidades JPA o declaram em `@Table`, e uma consulta JPQL que muda de schema por propriedade é o bug de `search_path` que o Barrier já pagou) e de um histórico
  Flyway separado: `flyway_schema_history_webhook_delivery`, migrations em
  `classpath:db/migration/webhook-delivery`. Bean `Flyway` próprio, executado
  na subida antes do JPA validar.
- `V1__inicial.sql` consolida as V001–V008 do Barrier **já no formato novo**
  (`aggregate_id`, `endpoint_id`, `events`, sem `job_locks`, que é do
  Barrier).
- **No Barrier**: uma migração `V009__move_para_webhook_delivery.sql` no
  `webhook-api` move `deliveries` e `webhook_endpoints` para o schema da lib,
  renomeia `assessment_id → aggregate_id`, cria `id`/`events={"*"}` nos
  endpoints, preenche `endpoint_id` nas entregas pelo `tenant_id`, e a lib é
  configurada com `baseline-version=1` para reconhecer o schema como
  migrado. `job_locks` fica onde está.
- **Rejeitado — mesmo histórico Flyway do consumidor.** Dois produtos com
  histórias diferentes não conseguem compartilhar uma sequência de versões.

## 4. O que não muda

`HmacSigner` (formato, instante dentro da assinatura, instante da tentativa),
rotação com janela, as três travas de `claimDue`, lease, backoff com teto em
64x, `DEAD`, semáforo como teto de entregas simultâneas, virtual threads,
validação de URL (TLS fora de host local). Cada um tem o comentário de
incidente que o justifica; os comentários migram junto.

## 5. ArchUnit da lib

- `domain` não importa Spring, JPA nem `repository`/`service`/`client`.
- `client` não importa `repository`.
- Nenhum pacote importa `com.barrier.commons` nem `org.apache.kafka`.
- Entidades JPA são `package-private` em `repository`.

## 6. Ordem de execução e critério de pronto

1. **Repo `webhook-delivery`**: código movido e generalizado, testes movidos
   e verdes (Testcontainers), ArchUnit verde, `V1` escrita, autoconfig,
   README com o contrato (`DeliveryRequest`, headers, formato da assinatura,
   propriedades). CI publica `0.1.0` no GitHub Packages.
2. **Barrier adota `0.1.0`**: `webhook-api` troca os pacotes pela
   dependência, `V009`, listener monta `DeliveryRequest`, controller chama
   `registerSingle`. **Critério**: a suíte do `webhook-api` que ficou
   (controller, listener, API, reconciliação) passa **sem alteração de
   asserção**; `X-Barrier-Signature` continua o header; um consumidor de
   webhook do Barrier não percebe a troca.
3. **Gateway** consome `0.1.x` como §7 da spec dele.

Fora de escopo: entrega por outro transporte que não HTTP, fan-out por
evento sem tenant, UI de entregas, migração automática do `job_locks`.
