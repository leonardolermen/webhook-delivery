# webhook-delivery

Entrega de webhooks assinados a endpoints de clientes: retry com backoff, ordenação por chave,
idempotência por (evento, endpoint), rotação de segredo com janela. Extraída do Barrier
(`services/webhook-api`); a história de cada decisão está nos comentários do código e em
`docs/superpowers/specs/`.

## Uso

```xml
<dependency>
  <groupId>com.barrier</groupId>
  <artifactId>webhook-delivery</artifactId>
  <version>0.1.0</version>
</dependency>
```

GitHub Packages exige token até para ler. Em `~/.m2/settings.xml`:

```xml
<servers><server><id>github-webhook-delivery</id><username>SEU_USUARIO</username><password>TOKEN_COM_read:packages</password></server></servers>
```
e no `pom.xml` do consumidor:
```xml
<repositories><repository><id>github-webhook-delivery</id><url>https://maven.pkg.github.com/leonardolermen/webhook-delivery</url></repository></repositories>
```

No consumidor: `@EnableScheduling` na aplicação (o retry é `@Scheduled`); se você declara
`@EntityScan`/`@EnableJpaRepositories`, inclua `com.barrier.webhookdelivery.repository`.

A autoconfiguração registra `com.barrier.webhookdelivery.repository` para o scan de entidades e
repositórios Spring Data. Aplicação cujo `@SpringBootApplication` está num pacote **ancestral** do da
lib (ex.: `com.barrier`) já escaneia esse pacote pelo próprio auto-configuration package, e os dois
registros colidem (`BeanDefinitionOverrideException` em `DeliveryJpaRepository`). Nesse caso, mova a
classe da aplicação para um subpacote (ex.: `com.barrier.meuservico`) ou declare
`@EnableJpaRepositories`/`@EntityScan` restritos aos seus pacotes mais
`com.barrier.webhookdelivery.repository`.

```java
@Autowired DeliveryIntake intake;
intake.accept(new DeliveryRequest(tenantId, "payment.completed", eventId, "pay_1", "pay_1", json, correlationId));
```

## Propriedades (`webhook-delivery.*`)

| propriedade | default | o quê |
|---|---|---|
| `workers` | 16 | entregas simultâneas. Threads virtuais: o custo real é o pool de conexões, que cada tentativa usa por milissegundos (resolver o segredo e gravar o desfecho), então o valor pode passar o tamanho do pool |
| `max-in-flight-per-endpoint` | 4 | teto de entregas simultâneas de um mesmo endpoint; um parceiro fora do ar não ocupa todos os workers |
| `lease` | PT2M | posse de uma entrega por um worker; precisa cobrir **uma** tentativa (connect+read timeout), pois a reivindicação nunca excede os `workers` livres |
| `retry-delay-ms` | 5000 | intervalo do scheduler |
| `max-attempts` | 5 | depois disso a entrega vira `DEAD` |
| `base-backoff` | PT30S | backoff exponencial, teto 64x |
| `connect-timeout` / `read-timeout` | PT2S / PT10S | |
| `allow-private-targets` | false | **só dev.** Desliga a política de destino (SSRF): por padrão o registro e cada envio recusam URL cujo host resolve para loopback, rede privada, link-local ou ULA |
| `secret-rotation-overlap` | PT24H | janela em que o segredo anterior ainda assina |
| `headers.prefix` | `X-Webhook` | `-Signature`, `-Signature-Previous`, `-Event-Id`, `-Event-Type` |
| `correlation-mdc-key` | `correlationId` | |
| `scheduler.enabled` | true | |
| `flyway.baseline-on-migrate` / `flyway.baseline-version` | false / 1 | para schema pré-existente |

## Assinatura

`<prefix>-Signature: t=<epoch-segundos>,v1=<hex HMAC-SHA256(secret, t + "." + body)>`.
Verifique com o `t=` do header, rejeite se for velho demais — recomendamos uma janela de tolerância
de 5 minutos, que cobre desvio de relógio e rejeita replay. Durante rotação, `-Signature-Previous`
traz a assinatura pelo segredo anterior.

## Persistência

Schema `webhook_delivery`, histórico Flyway `flyway_schema_history_webhook_delivery`, migrations
próprias em `classpath:db/webhook-delivery` (fora de `db/migration`, que o Flyway do consumidor varre
recursivamente). A lib traz o próprio Flyway e **convive** com o do consumidor: ela não expõe bean
`Flyway` (o do Boot continua rodando o `db/migration` do consumidor) e roda **depois** dele quando ele
existe. Consumidor com schema já existente (Barrier): mova as tabelas numa migration sua e use
`flyway.baseline-on-migrate=true`.

## Desenvolvimento

`./mvnw verify` (Testcontainers; precisa de Docker). Publicação: tag `vX.Y.Z` no `main`.
