# webhook-delivery — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extrair a máquina de entrega de webhooks do `services/webhook-api` do Barrier para a biblioteca Maven `com.barrier:webhook-delivery`, generalizada em quatro pontos (entrada abstrata, N endpoints por tenant com filtro de evento, nomes configuráveis, schema/Flyway próprios), publicada como `0.1.0`, e adotada pelo Barrier sem mudar o contrato dos parceiros dele.

**Architecture:** Mudança de endereço, não reescrita: cada arquivo é copiado de `C:\Dev\barrier\services\webhook-api\src\main\java\com\barrier\webhook\` para `com.barrier.webhookdelivery`, com o comentário de incidente intacto, e recebe só os diffs listados na tarefa. A lib expõe uma `DeliveryIntake` e uma autoconfiguração Spring com `@Import` explícito; a persistência mora no schema fixo `webhook_delivery` com histórico Flyway próprio. Domínio sem Spring/JPA; entidades package-private; nada de `commons` nem Kafka.

**Tech Stack:** Java 25, Spring Boot 4.0.7 (`spring-boot-starter-parent`), Spring Data JPA/Hibernate, Flyway, PostgreSQL, `RestClient` sobre JDK `HttpClient`, Lombok (só nas entidades, como no Barrier), JUnit 5, AssertJ, Testcontainers (Postgres 17), ArchUnit 1.5.0, Awaitility. Maven com wrapper. GitHub Actions + GitHub Packages.

**Spec:** `docs/superpowers/specs/2026-09-23-extracao-webhook-delivery-design.md` (este repo). Consumidor final: `C:\Dev\payment-gateway\docs\superpowers\specs\2026-09-23-payment-gateway-design.md` §7.

## Global Constraints

- Coordenadas `com.barrier:webhook-delivery`, pacote raiz `com.barrier.webhookdelivery`, versão inicial `0.1.0-SNAPSHOT` → tag `v0.1.0`.
- Java 25, Spring Boot 4.0.7, ArchUnit 1.5.0, Testcontainers 1.21.4 — os mesmos do `C:\Dev\barrier\pom.xml`.
- A lib **não depende** de `com.barrier:commons`, `spring-kafka` nem `org.apache.kafka` (ArchUnit cobra).
- Schema **fixo** `webhook_delivery`; histórico Flyway `flyway_schema_history_webhook_delivery`; migrations em `classpath:db/migration/webhook-delivery`.
- Prefixo de propriedades `webhook-delivery.*`. Headers `${headers.prefix}-Signature`, `-Signature-Previous`, `-Event-Id`, `-Event-Type`; prefixo default `X-Webhook`.
- Formato da assinatura `t=<epoch-segundos>,v1=<hex HMAC-SHA256(secret, epoch + "." + body)>` **não muda**.
- Comentários em português, registrando POR QUÊ com evidência; os comentários de incidente do Barrier migram verbatim. Commits `tipo(escopo): frase em minuscula` com corpo explicando o porquê.
- Nenhum teste fala com a rede externa: destinos de webhook são `HttpServer` da JDK em `localhost`.
- Origem de cada arquivo copiado: `C:\Dev\barrier\services\webhook-api\src\main\java\com\barrier\webhook\` (abreviado abaixo como `BARRIER/`) e testes em `C:\Dev\barrier\services\webhook-api\src\test\java\com\barrier\webhook\` (`BARRIER_TEST/`).

---

## Estrutura de arquivos

```
webhook-delivery/
  pom.xml
  mvnw, mvnw.cmd, .mvn/wrapper/*                 copiados de C:\Dev\barrier
  .gitignore
  README.md
  .github/workflows/ci.yml
  src/main/java/com/barrier/webhookdelivery/
    domain/       Delivery, DeliveryStatus, WebhookEndpoint, SigningMaterial, EventTypeMatcher
    intake/       DeliveryIntake, DeliveryRequest, IntakeResult
    repository/   DeliveryEntity, DeliveryEntityMapper, DeliveryJpaRepository, DeliveryRepository,
                  DeliveryRepositoryImpl, WebhookEndpointEntity, WebhookEndpointJpaRepository,
                  WebhookEndpointRepository, WebhookEndpointRepositoryImpl
    service/      WebhookDeliveryService (implements DeliveryIntake), WebhookEndpointService,
                  DeliveryRetryScheduler
    client/       HmacSigner, HttpWebhookClient, WebhookClient, WebhookRequest, WebhookSendResult
    observability/ Correlation
    config/       WebhookDeliveryProperties, WebhookDeliveryFlywayConfiguration,
                  WebhookDeliveryAutoConfiguration
  src/main/resources/
    META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
    db/migration/webhook-delivery/V1__inicial.sql
  src/test/java/com/barrier/webhookdelivery/
    TestApplication
    domain/       DeliveryPartitionKeyTest, EventTypeMatcherTest, WebhookEndpointTest
    client/       HmacSignerTest
    repository/   DeliveryOrderingIntegrationTest, DeliveryClaimExclusionIntegrationTest
    service/      WebhookDeliveryIntegrationTest, WebhookEndpointServiceIntegrationTest
    architecture/ ArchitectureTest
  src/test/resources/archunit.properties
```

Responsabilidades: `domain` decide (transições, validação de URL, match de evento); `repository` persiste e reivindica; `service` orquestra (fan-out, tentativa, assinatura); `client` fala HTTP; `config` liga tudo no Spring do consumidor.

---

### Task 1: Esqueleto do projeto que compila e sobe um contexto vazio

**Files:**
- Create: `pom.xml`, `.gitignore`, `src/test/java/com/barrier/webhookdelivery/TestApplication.java`, `src/test/resources/archunit.properties`
- Copy: `C:\Dev\barrier\mvnw`, `mvnw.cmd`, `.mvn\` → raiz do repo

**Interfaces:**
- Produces: projeto Maven `com.barrier:webhook-delivery:0.1.0-SNAPSHOT`; `TestApplication` (`@SpringBootApplication` em `com.barrier.webhookdelivery`) que os testes de integração usam.

- [ ] **Step 1: Copiar o wrapper do Maven**

```bash
cd /c/Dev/webhook-delivery && cp /c/Dev/barrier/mvnw /c/Dev/barrier/mvnw.cmd . && cp -r /c/Dev/barrier/.mvn . && ls .mvn/wrapper
```
Expected: `maven-wrapper.properties` listado.

- [ ] **Step 2: Escrever `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.0.7</version>
        <relativePath/>
    </parent>

    <groupId>com.barrier</groupId>
    <artifactId>webhook-delivery</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>jar</packaging>
    <name>webhook-delivery</name>
    <description>Entrega de webhooks assinados com retry, ordenação por chave, idempotência e rotação de segredo</description>

    <properties>
        <java.version>25</java.version>
        <archunit.version>1.5.0</archunit.version>
        <testcontainers.version>1.21.4</testcontainers.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <!-- RestClient. Só spring-web, não o starter-web: a lib não sobe servidor nenhum. -->
        <dependency>
            <groupId>org.springframework</groupId>
            <artifactId>spring-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-autoconfigure</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.tngtech.archunit</groupId>
            <artifactId>archunit-junit5</artifactId>
            <version>${archunit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>${lombok.version}</version>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

Se o Barrier declara `spring-boot-starter-test` de outra forma ou o `spring-boot-testcontainers` tiver outro `artifactId` em Boot 4.0.7, copie a forma do `C:\Dev\barrier\services\webhook-api\pom.xml` — ela compila hoje.

- [ ] **Step 3: `.gitignore`, `archunit.properties` e `TestApplication`**

`.gitignore`:
```
target/
.idea/
*.iml
.vscode/
```

`src/test/resources/archunit.properties`:
```
archRule.failOnEmptyShould=false
```

`src/test/java/com/barrier/webhookdelivery/TestApplication.java`:
```java
package com.barrier.webhookdelivery;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Aplicação mínima só para os testes de integração: a lib não tem main. Escaneia apenas este
 * pacote raiz de teste — as classes da lib entram pela autoconfiguração, exatamente como num
 * consumidor real. Se a autoconfig deixar de registrar algo, é aqui que o teste quebra.
 */
@SpringBootApplication(scanBasePackages = "com.barrier.webhookdelivery.testapp")
@EnableScheduling
public class TestApplication {}
```

- [ ] **Step 4: Compilar**

Run: `cd /c/Dev/webhook-delivery && ./mvnw -B -q -DskipTests package`
Expected: `BUILD SUCCESS` (sem classes ainda; o jar sai vazio).

- [ ] **Step 5: Commit**

```bash
git add pom.xml .gitignore mvnw mvnw.cmd .mvn src/test
git commit -m "build: esqueleto maven da lib, mesma toolchain do barrier

Java 25 e Spring Boot 4.0.7 porque o primeiro consumidor e o Barrier e
compartilhar biblioteca entre majors diferentes do Spring nao funciona.
Sem starter-web: a lib nao sobe servidor."
```

---

### Task 2: Domínio — `DeliveryStatus`, `SigningMaterial`, `HmacSigner`

**Files:**
- Create: `src/main/java/com/barrier/webhookdelivery/domain/DeliveryStatus.java`, `domain/SigningMaterial.java`, `client/HmacSigner.java`
- Test: `src/test/java/com/barrier/webhookdelivery/client/HmacSignerTest.java`

**Interfaces:**
- Produces: `enum DeliveryStatus { PENDING, FAILED, DELIVERED, DEAD }`; `record SigningMaterial(String secret, String previousSecret)` com `hasPrevious()`; `HmacSigner.sign(String body, String secret, Instant instant) -> String`.

- [ ] **Step 1: Copiar o teste do signer**

Copie `BARRIER_TEST/client/HmacSignerTest.java` para `src/test/java/com/barrier/webhookdelivery/client/HmacSignerTest.java` e troque `package com.barrier.webhook.client;` por `package com.barrier.webhookdelivery.client;`. Nenhum outro import muda (o teste só usa JUnit/AssertJ e o próprio `HmacSigner`).

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw -B -q test -Dtest=HmacSignerTest`
Expected: erro de compilação — `HmacSigner` não existe.

- [ ] **Step 3: Copiar as três classes**

- `BARRIER/domain/DeliveryStatus.java` → `domain/DeliveryStatus.java`, pacote `com.barrier.webhookdelivery.domain`. Conteúdo: o enum como está no Barrier (`PENDING, FAILED, DELIVERED, DEAD`).
- `BARRIER/domain/SigningMaterial.java` → `domain/SigningMaterial.java`, pacote novo. No javadoc do campo `secret`, remova "(ou o global, em desenvolvimento)" — o segredo global sai da lib.
- `BARRIER/client/HmacSigner.java` → `client/HmacSigner.java`, pacote novo. **Remova** a anotação `@Component` e o import `org.springframework.stereotype.Component`: o bean é declarado na autoconfig (Task 9), e assim `client` não depende de Spring. O javadoc inteiro (instante dentro da assinatura, instante da tentativa, `v1=`) fica verbatim; só troque `X-Barrier-Signature-Previous` por `-Signature-Previous` no texto.

- [ ] **Step 4: Rodar e ver passar**

Run: `./mvnw -B -q test -Dtest=HmacSignerTest`
Expected: `Tests run: N, Failures: 0`.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/domain src/main/java/com/barrier/webhookdelivery/client src/test/java/com/barrier/webhookdelivery/client
git commit -m "feat(domain): status, material de assinatura e signer vindos do barrier

Copia literal com o javadoc de incidente intacto. HmacSigner perde o
@Component: o bean nasce na autoconfiguracao para o pacote client nao
depender de Spring."
```

---

### Task 3: Domínio — `Delivery` com `endpointId`, `eventType`, `aggregateId`

**Files:**
- Create: `src/main/java/com/barrier/webhookdelivery/domain/Delivery.java`
- Test: `src/test/java/com/barrier/webhookdelivery/domain/DeliveryPartitionKeyTest.java`, `domain/DeliveryTest.java`

**Interfaces:**
- Produces:
  ```java
  static Delivery create(UUID eventId, UUID endpointId, String eventType, String aggregateId,
                         String tenantId, String targetUrl, String payload, String partitionKey)
  static Delivery rehydrate(UUID id, UUID eventId, UUID endpointId, String eventType, String aggregateId,
                            String tenantId, String targetUrl, String payload, String partitionKey,
                            DeliveryStatus status, int attempts, String lastError, Instant nextAttemptAt,
                            Instant claimedAt, Instant createdAt, Instant deliveredAt)
  void markDelivered(); void markFailed(String error, int maxAttempts, Instant nextAttemptAt); void markDead(String error)
  // getters: id(), eventId(), endpointId(), eventType(), aggregateId(), tenantId(), targetUrl(), payload(),
  //          partitionKey(), status(), attempts(), lastError(), nextAttemptAt(), claimedAt(), createdAt(), deliveredAt()
  ```

- [ ] **Step 1: Copiar `DeliveryPartitionKeyTest` e escrever `DeliveryTest`**

Copie `BARRIER_TEST/domain/DeliveryPartitionKeyTest.java` para `src/test/java/com/barrier/webhookdelivery/domain/`, pacote novo. Onde o teste chama `Delivery.create(...)`, ajuste para a assinatura nova: `Delivery.create(UUID.randomUUID(), UUID.randomUUID(), "assessment.completed", "a-1", "tenant", "https://x/webhook", "{}", chave)` — mantendo a asserção sobre `partitionKey()`.

`DeliveryTest.java`:
```java
package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DeliveryTest {

  private static Delivery nova() {
    return Delivery.create(
        UUID.randomUUID(), UUID.randomUUID(), "payment.completed", "pay_1",
        "merchant-1", "https://x/webhook", "{}", "pay_1");
  }

  @Test
  void nasceLivrePendenteEVencida() {
    Delivery d = nova();
    assertThat(d.status()).isEqualTo(DeliveryStatus.PENDING);
    assertThat(d.claimedAt()).isNull();
    assertThat(d.nextAttemptAt()).isEqualTo(d.createdAt());
    assertThat(d.eventType()).isEqualTo("payment.completed");
  }

  @Test
  void falhaReagendaAteEsgotarEDepoisMorre() {
    Delivery d = nova();
    d.markFailed("HTTP 500", 2, Instant.now().plusSeconds(30));
    assertThat(d.status()).isEqualTo(DeliveryStatus.FAILED);
    assertThat(d.attempts()).isEqualTo(1);
    d.markFailed("HTTP 500", 2, Instant.now().plusSeconds(60));
    assertThat(d.status()).isEqualTo(DeliveryStatus.DEAD);
    assertThat(d.nextAttemptAt()).isNull();
  }

  /** Endpoint desativado entre a criacao e a tentativa: nao ha o que retentar. */
  @Test
  void markDeadEncerraSemConsumirTentativas() {
    Delivery d = nova();
    d.markDead("endpoint desativado");
    assertThat(d.status()).isEqualTo(DeliveryStatus.DEAD);
    assertThat(d.lastError()).isEqualTo("endpoint desativado");
    assertThat(d.claimedAt()).isNull();
  }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw -B -q test -Dtest='Delivery*Test'`
Expected: erro de compilação — `Delivery` não existe.

- [ ] **Step 3: Copiar `Delivery` e aplicar o diff**

Copie `BARRIER/domain/Delivery.java` para `domain/Delivery.java`, pacote novo, e:
1. Substitua o campo `private final String assessmentId;` por três campos: `private final UUID endpointId; private final String eventType; private final String aggregateId;`.
2. No construtor privado, em `create` e em `rehydrate`, substitua o parâmetro `String assessmentId` por `UUID endpointId, String eventType, String aggregateId` (nesta ordem, logo após `eventId`), e as atribuições correspondentes.
3. Substitua o getter `assessmentId()` por `endpointId()`, `eventType()`, `aggregateId()`.
4. Adicione depois de `markFailed`:
```java
  /**
   * Encerra sem retentativa. Existe para o endpoint que foi desativado entre a criação da entrega e
   * a tentativa: reagendar consumiria as tentativas todas entregando para um destino que o próprio
   * tenant desligou.
   */
  public void markDead(String error) {
    this.status = DeliveryStatus.DEAD;
    this.lastError = error;
    this.claimedAt = null;
    this.nextAttemptAt = null;
  }
```
5. No javadoc de `partitionKey`, troque "É o subject, não o tenant" por "No Barrier é o subject, no gateway é o payment — nunca o tenant" (o resto fica).
6. O comentário "Nasce LIVRE, e isto inverte uma regra anterior" fica verbatim.

- [ ] **Step 4: Rodar e ver passar**

Run: `./mvnw -B -q test -Dtest='Delivery*Test'`
Expected: todos verdes.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/domain/Delivery.java src/test/java/com/barrier/webhookdelivery/domain
git commit -m "feat(domain): delivery conhece endpoint, tipo de evento e agregado

assessment_id vira aggregate_id porque a lib nao sabe o que e uma
avaliacao. endpoint_id entra porque a idempotencia passa a ser por
(evento, endpoint): um tenant com dois endpoints recebe o evento duas
vezes, uma em cada. markDead cobre o endpoint desativado no meio."
```

---

### Task 4: Domínio — `WebhookEndpoint` com `id`, `events[]` e `EventTypeMatcher`

**Files:**
- Create: `domain/WebhookEndpoint.java`, `domain/EventTypeMatcher.java`
- Test: `src/test/java/com/barrier/webhookdelivery/domain/EventTypeMatcherTest.java`, `domain/WebhookEndpointTest.java`

**Interfaces:**
- Produces:
  ```java
  record WebhookEndpoint(UUID id, String tenantId, String targetUrl, String secret, String previousSecret,
                         Instant previousSecretUntil, List<String> events, boolean active, Instant createdAt, Instant updatedAt)
  static WebhookEndpoint register(String tenantId, String targetUrl, List<String> events)
  WebhookEndpoint withTargetUrl(String targetUrl); WebhookEndpoint withEvents(List<String> events)
  WebhookEndpoint rotateSecret(Duration overlap); WebhookEndpoint deactivate()
  String usablePreviousSecret(); boolean subscribedTo(String eventType)
  static final List<String> ALL_EVENTS = List.of("*")
  EventTypeMatcher.matches(List<String> subscribed, String eventType) -> boolean
  ```

- [ ] **Step 1: Testes**

`EventTypeMatcherTest.java`:
```java
package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class EventTypeMatcherTest {

  @Test
  void curingaSozinhoCasaComTudo() {
    assertThat(EventTypeMatcher.matches(List.of("*"), "payment.completed")).isTrue();
  }

  @Test
  void igualdadeExata() {
    assertThat(EventTypeMatcher.matches(List.of("payment.completed"), "payment.completed")).isTrue();
    assertThat(EventTypeMatcher.matches(List.of("payment.completed"), "payment.pending")).isFalse();
  }

  @Test
  void prefixoComCuringaFinal() {
    assertThat(EventTypeMatcher.matches(List.of("payment.*"), "payment.completed")).isTrue();
    assertThat(EventTypeMatcher.matches(List.of("payment.*"), "refund.completed")).isFalse();
  }

  /** Só curinga FINAL: "*.completed" e "pay*ment" não são suportados, de propósito (sem glob). */
  @Test
  void curingaNoMeioOuNoInicioNaoCasa() {
    assertThat(EventTypeMatcher.matches(List.of("*.completed"), "payment.completed")).isFalse();
    assertThat(EventTypeMatcher.matches(List.of("pay*ment"), "payment")).isFalse();
  }

  @Test
  void listaVaziaOuNulaNaoCasaComNada() {
    assertThat(EventTypeMatcher.matches(List.of(), "payment.completed")).isFalse();
    assertThat(EventTypeMatcher.matches(null, "payment.completed")).isFalse();
  }
}
```

`WebhookEndpointTest.java`:
```java
package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class WebhookEndpointTest {

  @Test
  void registroNasceAtivoComIdESegredo() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of("payment.*"));
    assertThat(e.id()).isNotNull();
    assertThat(e.secret()).isNotBlank();
    assertThat(e.active()).isTrue();
    assertThat(e.subscribedTo("payment.completed")).isTrue();
    assertThat(e.subscribedTo("refund.completed")).isFalse();
  }

  @Test
  void eventsVazioViraTodos() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of());
    assertThat(e.events()).isEqualTo(WebhookEndpoint.ALL_EVENTS);
  }

  @Test
  void trocarUrlPreservaSegredoEId() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of("*"));
    WebhookEndpoint f = e.withTargetUrl("https://acme/v2/webhook");
    assertThat(f.id()).isEqualTo(e.id());
    assertThat(f.secret()).isEqualTo(e.secret());
    assertThat(f.targetUrl()).isEqualTo("https://acme/v2/webhook");
  }

  @Test
  void rotacaoMantemAnteriorPelaJanela() {
    WebhookEndpoint e = WebhookEndpoint.register("t1", "https://acme/webhook", List.of("*"));
    WebhookEndpoint r = e.rotateSecret(Duration.ofHours(1));
    assertThat(r.secret()).isNotEqualTo(e.secret());
    assertThat(r.usablePreviousSecret()).isEqualTo(e.secret());
  }

  @Test
  void httpForaDeLocalhostEhRecusado() {
    assertThatThrownBy(() -> WebhookEndpoint.register("t1", "http://acme/webhook", List.of("*")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sem TLS");
  }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw -B -q test -Dtest='EventTypeMatcherTest,WebhookEndpointTest'`
Expected: erro de compilação.

- [ ] **Step 3: `EventTypeMatcher`**

```java
package com.barrier.webhookdelivery.domain;

import java.util.List;

/**
 * Decide se um endpoint inscrito em {@code subscribed} recebe {@code eventType}.
 *
 * <p>Três formas e só elas: {@code *} (tudo), igualdade exata e prefixo com curinga <b>final</b>
 * ({@code payment.*}). Glob geral foi rejeitado: cada forma extra é um jeito a mais de um parceiro
 * achar que está inscrito e não estar, e o sintoma é "não recebi o webhook" — o mais caro de
 * investigar.
 */
public final class EventTypeMatcher {

  private EventTypeMatcher() {}

  public static boolean matches(List<String> subscribed, String eventType) {
    if (subscribed == null || eventType == null) {
      return false;
    }
    for (String pattern : subscribed) {
      if (pattern == null) {
        continue;
      }
      if (pattern.equals("*") || pattern.equals(eventType)) {
        return true;
      }
      if (pattern.endsWith(".*")
          && pattern.indexOf('*') == pattern.length() - 1
          && eventType.startsWith(pattern.substring(0, pattern.length() - 1))) {
        return true;
      }
    }
    return false;
  }
}
```

- [ ] **Step 4: `WebhookEndpoint`**

Copie `BARRIER/domain/WebhookEndpoint.java` para `domain/WebhookEndpoint.java`, pacote novo, e:
1. Record passa a `WebhookEndpoint(UUID id, String tenantId, String targetUrl, String secret, String previousSecret, Instant previousSecretUntil, List<String> events, boolean active, Instant createdAt, Instant updatedAt)`. Adicione `import java.util.List; import java.util.UUID;`.
2. Adicione `public static final List<String> ALL_EVENTS = List.of("*");`.
3. Substitua os dois `register(...)` por:
```java
  public static WebhookEndpoint register(String tenantId, String targetUrl, List<String> events) {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId obrigatório");
    }
    validate(targetUrl);
    Instant now = Instant.now();
    return new WebhookEndpoint(
        UUID.randomUUID(), tenantId, targetUrl.trim(), newSecret(), null, null,
        normalize(events), true, now, now);
  }

  /** Atualização de URL <b>preserva o segredo</b>: trocar de quebra derrubaria a verificação do cliente sem ninguém ter pedido rotação. */
  public WebhookEndpoint withTargetUrl(String newTargetUrl) {
    validate(newTargetUrl);
    return new WebhookEndpoint(id, tenantId, newTargetUrl.trim(), secret, previousSecret,
        previousSecretUntil, events, active, createdAt, Instant.now());
  }

  public WebhookEndpoint withEvents(List<String> newEvents) {
    return new WebhookEndpoint(id, tenantId, targetUrl, secret, previousSecret,
        previousSecretUntil, normalize(newEvents), active, createdAt, Instant.now());
  }

  public boolean subscribedTo(String eventType) {
    return EventTypeMatcher.matches(events, eventType);
  }

  /** Lista vazia ou nula significa "tudo": o caso do Barrier, que tem um endpoint por tenant e nunca filtrou. */
  private static List<String> normalize(List<String> events) {
    if (events == null || events.isEmpty()) {
      return ALL_EVENTS;
    }
    return List.copyOf(events);
  }
```
4. Em `rotateSecret` e `deactivate`, reescreva o `new WebhookEndpoint(...)` com os campos na ordem nova (`id, tenantId, targetUrl, …, events, active, createdAt, updatedAt`).
5. `validate`, `newSecret`, `usablePreviousSecret` e os javadocs ficam verbatim. Na mensagem de `validate` a frase "sem TLS" precisa continuar existindo (o teste procura por ela).

- [ ] **Step 5: Rodar e ver passar**

Run: `./mvnw -B -q test -Dtest='EventTypeMatcherTest,WebhookEndpointTest,Delivery*Test'`
Expected: verdes.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/domain src/test/java/com/barrier/webhookdelivery/domain
git commit -m "feat(domain): endpoint com id proprio e filtro por tipo de evento

Um tenant passa a poder ter N endpoints, cada um inscrito numa lista de
eventos. Match e so igualdade, * e prefixo com curinga final: glob geral
e um jeito a mais de o parceiro achar que esta inscrito e nao estar."
```

---

### Task 5: Contrato de entrada — `intake/` e `observability/Correlation`

**Files:**
- Create: `intake/DeliveryIntake.java`, `intake/DeliveryRequest.java`, `intake/IntakeResult.java`, `observability/Correlation.java`
- Test: `src/test/java/com/barrier/webhookdelivery/observability/CorrelationTest.java`

**Interfaces:**
- Produces: exatamente os tipos do §3.1 da spec; `Correlation.run(String mdcKey, String correlationId, Runnable action)` e `Correlation.current(String mdcKey)`.

- [ ] **Step 1: Teste da correlação**

```java
package com.barrier.webhookdelivery.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class CorrelationTest {

  @Test
  void colocaNoMdcERestauraOAnterior() {
    MDC.put("cid", "antes");
    Correlation.run("cid", "durante", () -> assertThat(MDC.get("cid")).isEqualTo("durante"));
    assertThat(MDC.get("cid")).isEqualTo("antes");
    MDC.remove("cid");
  }

  @Test
  void semIdNaoTocaNoMdcELimpaAoFinal() {
    Correlation.run("cid", null, () -> assertThat(MDC.get("cid")).isNull());
    assertThat(MDC.get("cid")).isNull();
  }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `./mvnw -B -q test -Dtest=CorrelationTest` — Expected: erro de compilação.

- [ ] **Step 3: Implementar**

`intake/DeliveryRequest.java`:
```java
package com.barrier.webhookdelivery.intake;

import java.util.Objects;
import java.util.UUID;

/**
 * Pedido de entrega de um evento aos endpoints de um tenant.
 *
 * @param tenantId dono do evento; é dele que saem os endpoints e os segredos
 * @param eventType nome canônico ({@code payment.completed}); é o que os endpoints filtram
 * @param eventId idempotência: o mesmo evento nunca gera duas entregas para o mesmo endpoint
 * @param aggregateId id do agregado de origem, só para rastreio; a lib não interpreta
 * @param partitionKey chave de ordenação; entregas com a mesma chave nunca correm em paralelo.
 *     {@code null} = sem ordem exigida — fail-open, o desconhecido não trava a fila
 * @param payload corpo exato que será assinado e entregue; a lib não o altera nem normaliza
 * @param correlationId id da requisição de origem, para o log; pode ser {@code null}
 */
public record DeliveryRequest(
    String tenantId,
    String eventType,
    UUID eventId,
    String aggregateId,
    String partitionKey,
    String payload,
    String correlationId) {

  public DeliveryRequest {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(payload, "payload");
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId obrigatório");
    }
    if (eventType == null || eventType.isBlank()) {
      throw new IllegalArgumentException("eventType obrigatório");
    }
    if (aggregateId == null || aggregateId.isBlank()) {
      throw new IllegalArgumentException("aggregateId obrigatório");
    }
  }
}
```

`intake/IntakeResult.java`:
```java
package com.barrier.webhookdelivery.intake;

/**
 * @param endpointsMatched endpoints ativos do tenant inscritos no evento
 * @param deliveriesCreated entregas efetivamente registradas (menor que matched quando o evento já
 *     tinha entrega para algum endpoint — idempotência)
 */
public record IntakeResult(int endpointsMatched, int deliveriesCreated) {
  public static final IntakeResult NONE = new IntakeResult(0, 0);
}
```

`intake/DeliveryIntake.java`:
```java
package com.barrier.webhookdelivery.intake;

/** Porta de entrada da biblioteca: quem tem um evento para entregar chama isto. */
public interface DeliveryIntake {

  /**
   * Registra uma entrega para cada endpoint ativo do tenant inscrito em {@code eventType}.
   * Idempotente por {@code (eventId, endpointId)}. <b>Não entrega aqui</b>: a entrega é feita pelo
   * pool, fora da thread de quem chamou — ver {@code WebhookDeliveryService#retryDue}.
   */
  IntakeResult accept(DeliveryRequest request);
}
```

`observability/Correlation.java` — copie `C:\Dev\barrier\commons\src\main\java\com\barrier\commons\observability\Correlation.java`, pacote `com.barrier.webhookdelivery.observability`, e:
- remova `MDC_KEY`, `current()` e `currentOrNew()`;
- `run` vira `public static void run(String mdcKey, String correlationId, Runnable action)` usando `mdcKey` no lugar de `MDC_KEY`;
- adicione `public static String current(String mdcKey) { return MDC.get(mdcKey); }`;
- troque o primeiro parágrafo do javadoc por: "Cópia enxuta do `Correlation` do Barrier: a lib não pode depender de `commons` (que puxa Kafka), e a chave do MDC é do consumidor — o Barrier usa `correlationId`, outro produto pode usar outra."

- [ ] **Step 4: Rodar e ver passar**

Run: `./mvnw -B -q test -Dtest=CorrelationTest` — Expected: verde.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/intake src/main/java/com/barrier/webhookdelivery/observability src/test/java/com/barrier/webhookdelivery/observability
git commit -m "feat(intake): contrato de entrada da lib e correlacao propria

DeliveryRequest substitui EventEnvelope + tenantId extraido do payload:
quem chama ja sabe tenant, tipo e chave de ordenacao. Correlation e
copia do commons com a chave do MDC parametrizada, para a lib nao
puxar Kafka por tabela."
```

---

### Task 6: Propriedades e `V1__inicial.sql`

**Files:**
- Create: `config/WebhookDeliveryProperties.java`, `src/main/resources/db/migration/webhook-delivery/V1__inicial.sql`
- Test: `src/test/java/com/barrier/webhookdelivery/config/WebhookDeliveryPropertiesTest.java`

**Interfaces:**
- Produces:
  ```java
  @ConfigurationProperties("webhook-delivery")
  record WebhookDeliveryProperties(int workers, Duration lease, long retryDelayMs, int maxAttempts, Duration baseBackoff,
      Duration connectTimeout, Duration readTimeout, Duration secretRotationOverlap, Headers headers,
      String correlationMdcKey, Scheduler scheduler, Flyway flyway)
  record Headers(String prefix) { String signature(); String previousSignature(); String eventId(); String eventType(); }
  record Scheduler(boolean enabled) ; record Flyway(boolean baselineOnMigrate, String baselineVersion)
  ```

- [ ] **Step 1: Teste dos defaults e dos nomes de header**

```java
package com.barrier.webhookdelivery.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WebhookDeliveryPropertiesTest {

  @Test
  void defaultsSaoOsDoBarrier() {
    WebhookDeliveryProperties p =
        new WebhookDeliveryProperties(0, null, 0, 0, null, null, null, null, null, null, null, null);
    assertThat(p.workers()).isEqualTo(3);
    assertThat(p.lease()).isEqualTo(Duration.ofMinutes(2));
    assertThat(p.retryDelayMs()).isEqualTo(5000);
    assertThat(p.maxAttempts()).isEqualTo(5);
    assertThat(p.baseBackoff()).isEqualTo(Duration.ofSeconds(30));
    assertThat(p.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
    assertThat(p.readTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(p.secretRotationOverlap()).isEqualTo(Duration.ofHours(24));
    assertThat(p.headers().prefix()).isEqualTo("X-Webhook");
    assertThat(p.correlationMdcKey()).isEqualTo("correlationId");
    assertThat(p.scheduler().enabled()).isTrue();
    assertThat(p.flyway().baselineOnMigrate()).isFalse();
    assertThat(p.flyway().baselineVersion()).isEqualTo("1");
  }

  @Test
  void headersDerivamDoPrefixo() {
    WebhookDeliveryProperties.Headers h = new WebhookDeliveryProperties.Headers("X-Barrier");
    assertThat(h.signature()).isEqualTo("X-Barrier-Signature");
    assertThat(h.previousSignature()).isEqualTo("X-Barrier-Signature-Previous");
    assertThat(h.eventId()).isEqualTo("X-Barrier-Event-Id");
    assertThat(h.eventType()).isEqualTo("X-Barrier-Event-Type");
  }
}
```

- [ ] **Step 2: Rodar e ver falhar** — `./mvnw -B -q test -Dtest=WebhookDeliveryPropertiesTest`, erro de compilação.

- [ ] **Step 3: Implementar as propriedades**

```java
package com.barrier.webhookdelivery.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuração da entrega. Os defaults são os que o Barrier rodava em produção — o valor de cada um
 * tem a conta no {@code application.yml} de lá (pool de conexões vs. workers, lease maior que o pior
 * caso de connect+read timeout).
 *
 * <p>Não há mais {@code target-url} nem {@code secret} globais: eram fallback de desenvolvimento e,
 * com dois tenants, entregavam o callback de um no endpoint do outro. Quem quiser um destino de dev
 * registra um endpoint na subida.
 */
@ConfigurationProperties(prefix = "webhook-delivery")
public record WebhookDeliveryProperties(
    int workers,
    Duration lease,
    long retryDelayMs,
    int maxAttempts,
    Duration baseBackoff,
    Duration connectTimeout,
    Duration readTimeout,
    Duration secretRotationOverlap,
    Headers headers,
    String correlationMdcKey,
    Scheduler scheduler,
    Flyway flyway) {

  public WebhookDeliveryProperties {
    if (workers <= 0) workers = 3;
    if (lease == null) lease = Duration.ofMinutes(2);
    if (retryDelayMs <= 0) retryDelayMs = 5000;
    if (maxAttempts <= 0) maxAttempts = 5;
    if (baseBackoff == null) baseBackoff = Duration.ofSeconds(30);
    if (connectTimeout == null) connectTimeout = Duration.ofSeconds(2);
    if (readTimeout == null) readTimeout = Duration.ofSeconds(10);
    if (secretRotationOverlap == null) secretRotationOverlap = Duration.ofHours(24);
    if (headers == null) headers = new Headers(null);
    if (correlationMdcKey == null || correlationMdcKey.isBlank()) correlationMdcKey = "correlationId";
    if (scheduler == null) scheduler = new Scheduler(true);
    if (flyway == null) flyway = new Flyway(false, null);
  }

  /** Prefixo dos headers: o Barrier usa {@code X-Barrier}, o gateway {@code X-Gateway}. */
  public record Headers(String prefix) {
    public Headers {
      if (prefix == null || prefix.isBlank()) prefix = "X-Webhook";
    }

    public String signature() { return prefix + "-Signature"; }
    public String previousSignature() { return prefix + "-Signature-Previous"; }
    public String eventId() { return prefix + "-Event-Id"; }
    public String eventType() { return prefix + "-Event-Type"; }
  }

  public record Scheduler(boolean enabled) {}

  /**
   * @param baselineOnMigrate para o consumidor cujo schema já existe (o Barrier, depois da V009
   *     dele): marca a versão {@code baselineVersion} como aplicada em vez de rodar a V1
   */
  public record Flyway(boolean baselineOnMigrate, String baselineVersion) {
    public Flyway {
      if (baselineVersion == null || baselineVersion.isBlank()) baselineVersion = "1";
    }
  }
}
```

Nota: o binding de `record` aninhado com `@ConfigurationProperties` funciona em Boot 3+/4 por construtor; `Scheduler(boolean)` recebe `false` quando a propriedade não existe e o compact constructor de `WebhookDeliveryProperties` só substitui quando o record inteiro é `null`. Por isso o default `enabled=true` é tratado no `Scheduler` **nulo**; para desligar, o consumidor põe `webhook-delivery.scheduler.enabled=false`. O teste `defaultsSaoOsDoBarrier` cobre o caso nulo.

- [ ] **Step 4: Escrever `V1__inicial.sql`**

```sql
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
```

- [ ] **Step 5: Rodar e ver passar** — `./mvnw -B -q test -Dtest=WebhookDeliveryPropertiesTest`, verde.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/config src/main/resources/db src/test/java/com/barrier/webhookdelivery/config
git commit -m "feat(config): propriedades webhook-delivery.* e migracao inicial consolidada

Defaults sao os que o Barrier rodava. Sem target-url/secret globais:
com dois tenants entregavam o callback de um no endpoint do outro. V1
consolida V001-V008 ja com endpoint_id, event_type e aggregate_id."
```

---

### Task 7: Camada de persistência + Flyway próprio + autoconfiguração mínima

**Files:**
- Create: `repository/DeliveryEntity.java`, `DeliveryEntityMapper.java`, `DeliveryJpaRepository.java`, `DeliveryRepository.java`, `DeliveryRepositoryImpl.java`, `WebhookEndpointEntity.java`, `WebhookEndpointJpaRepository.java`, `WebhookEndpointRepository.java`, `WebhookEndpointRepositoryImpl.java`; `config/WebhookDeliveryFlywayConfiguration.java`, `config/WebhookDeliveryAutoConfiguration.java`; `src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `src/test/java/com/barrier/webhookdelivery/repository/DeliveryOrderingIntegrationTest.java`, `DeliveryClaimExclusionIntegrationTest.java`, `WebhookEndpointRepositoryIntegrationTest.java`

**Interfaces:**
- Consumes: `Delivery`, `DeliveryStatus`, `WebhookEndpoint` (Tasks 3–4), `WebhookDeliveryProperties` (Task 6).
- Produces:
  ```java
  interface DeliveryRepository { Delivery save(Delivery d); boolean existsByEventId(UUID eventId);
      Optional<Delivery> findById(UUID id); List<Delivery> claimDue(Instant now, int limit, Duration lease); }
  interface WebhookEndpointRepository { WebhookEndpoint save(WebhookEndpoint e); Optional<WebhookEndpoint> findById(UUID id);
      List<WebhookEndpoint> findByTenantId(String tenantId); List<WebhookEndpoint> findActiveByTenantId(String tenantId); List<WebhookEndpoint> findAll(); }
  ```
  Autoconfig registra: os dois repositórios, o `Flyway` da lib e o `BeanFactoryPostProcessor` que faz o `entityManagerFactory` depender dele.

- [ ] **Step 1: Portar os testes de ordenação e exclusão**

Copie `BARRIER_TEST/DeliveryOrderingIntegrationTest.java` e `BARRIER_TEST/DeliveryClaimExclusionIntegrationTest.java` para `src/test/java/com/barrier/webhookdelivery/repository/`, pacote `com.barrier.webhookdelivery.repository`, e aplique em **ambos**:
1. imports `com.barrier.webhook.domain.Delivery` → `com.barrier.webhookdelivery.domain.Delivery`; `com.barrier.webhook.repository.DeliveryRepository` → mesmo pacote (remova o import).
2. `@MockitoBean com.barrier.webhook.service.DeliveryRetryScheduler scheduler;` → **remova a linha**; em vez disso anote a classe com `@SpringBootTest(properties = "webhook-delivery.scheduler.enabled=false")`. Mantenha o comentário explicando a corrida com o `@Scheduled`, trocando "Scheduler substituido por mock" por "Scheduler desligado por propriedade".
3. Todo `webhook.deliveries` no SQL → `webhook_delivery.deliveries`.
4. O `INSERT` do helper `grava(...)` vira:
```java
    jdbc.update(
        """
        INSERT INTO webhook_delivery.deliveries
               (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload,
                status, attempts, next_attempt_at, created_at, partition_key)
        VALUES (?, ?, ?, 'assessment.completed', 'a-1', 'default', 'http://localhost:9000', '{}',
                'PENDING', 0, now() - interval '1 minute', now(), ?)
        """,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        partitionKey);
```
5. Os javadocs de classe e de método (as três travas, a linha que nasce depois do SELECT) ficam verbatim; onde dizem "listener do Kafka", troque por "quem chama `accept`".

`WebhookEndpointRepositoryIntegrationTest.java`:
```java
package com.barrier.webhookdelivery.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class WebhookEndpointRepositoryIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired WebhookEndpointRepository repository;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints");
  }

  @Test
  void persisteEventsComoArrayEFiltraAtivosPorTenant() {
    WebhookEndpoint a = repository.save(WebhookEndpoint.register("t1", "https://a/hook", List.of("payment.*")));
    WebhookEndpoint b = repository.save(WebhookEndpoint.register("t1", "https://b/hook", List.of("*")));
    repository.save(WebhookEndpoint.register("t2", "https://c/hook", List.of("*")));
    repository.save(b.deactivate());

    assertThat(repository.findById(a.id())).get().extracting(WebhookEndpoint::events)
        .isEqualTo(List.of("payment.*"));
    assertThat(repository.findByTenantId("t1")).hasSize(2);
    assertThat(repository.findActiveByTenantId("t1")).extracting(WebhookEndpoint::id).containsExactly(a.id());
  }

  /** O schema da lib e o historico Flyway proprio existem: e a autoconfig que os cria. */
  @Test
  void flywayDaLibRodouNoSchemaProprio() {
    Integer versoes = jdbc.queryForObject(
        "SELECT count(*) FROM webhook_delivery.flyway_schema_history_webhook_delivery WHERE success", Integer.class);
    assertThat(versoes).isGreaterThanOrEqualTo(1);
  }
}
```

- [ ] **Step 2: Rodar e ver falhar** — `./mvnw -B -q test -Dtest='*IntegrationTest'`, erro de compilação.

- [ ] **Step 3: Entidades e mapper**

`repository/DeliveryEntity.java` — copie `BARRIER/repository/DeliveryEntity.java`, pacote novo, e:
- `@Table(name = "deliveries")` → `@Table(name = "deliveries", schema = "webhook_delivery")`.
- `@Column(name = "event_id", nullable = false, unique = true)` → `@Column(name = "event_id", nullable = false)` (a unicidade agora é composta e vive no SQL).
- Substitua o campo `assessmentId` por:
```java
  @Column(name = "endpoint_id", nullable = false)
  private UUID endpointId;

  @Column(name = "event_type", nullable = false, length = 120)
  private String eventType;

  @Column(name = "aggregate_id", nullable = false, length = 64)
  private String aggregateId;
```
- `tenant_id` passa a `nullable = false`.

`repository/DeliveryEntityMapper.java` — copie e troque `setAssessmentId(d.assessmentId())` por `setEndpointId(d.endpointId()); e.setEventType(d.eventType()); e.setAggregateId(d.aggregateId());` e, em `toDomain`, `e.getAssessmentId()` por `e.getEndpointId(), e.getEventType(), e.getAggregateId()` na posição correspondente da assinatura de `rehydrate` (Task 3).

`repository/WebhookEndpointEntity.java` — copie e:
- `@Table(name = "webhook_endpoints", schema = "webhook_delivery")`.
- O `@Id` sai de `tenantId` e vai para um novo campo `@Id @Column(name = "id", nullable = false) private UUID id;`. `tenantId` fica `@Column(name = "tenant_id", nullable = false, length = 40)`.
- Adicione:
```java
  /** text[] nativo do Postgres; Hibernate 6.1+ mapeia List<String> sem conversor. */
  @JdbcTypeCode(SqlTypes.ARRAY)
  @Column(name = "events", nullable = false, columnDefinition = "text[]")
  private List<String> events;
```
com `import org.hibernate.annotations.JdbcTypeCode; import org.hibernate.type.SqlTypes; import java.util.List;`.

- [ ] **Step 4: Repositórios**

`DeliveryJpaRepository` — copie verbatim (pacote novo). O javadoc de `tentarTravarReivindicacao` e de `selectClaimable` fica intacto; a JPQL não muda.

`DeliveryRepository` — copie e adicione `Optional<Delivery> findById(UUID id);`.

`DeliveryRepositoryImpl` — copie verbatim (pacote novo) e adicione:
```java
  @Override
  public Optional<Delivery> findById(UUID id) {
    return jpa.findById(id).map(DeliveryEntityMapper::toDomain);
  }
```

`WebhookEndpointJpaRepository`:
```java
package com.barrier.webhookdelivery.repository;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface WebhookEndpointJpaRepository extends JpaRepository<WebhookEndpointEntity, UUID> {
  List<WebhookEndpointEntity> findByTenantIdOrderByCreatedAtAsc(String tenantId);
  List<WebhookEndpointEntity> findByTenantIdAndActiveTrueOrderByCreatedAtAsc(String tenantId);
}
```

`WebhookEndpointRepository`:
```java
package com.barrier.webhookdelivery.repository;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Endpoints de callback registrados; N por tenant. */
public interface WebhookEndpointRepository {
  WebhookEndpoint save(WebhookEndpoint endpoint);
  Optional<WebhookEndpoint> findById(UUID id);
  /** Ordenado por criação: o mais antigo é o "endpoint único" que o Barrier conhece. */
  List<WebhookEndpoint> findByTenantId(String tenantId);
  List<WebhookEndpoint> findActiveByTenantId(String tenantId);
  List<WebhookEndpoint> findAll();
}
```

`WebhookEndpointRepositoryImpl` — copie e reescreva os métodos:
```java
  @Override
  public WebhookEndpoint save(WebhookEndpoint endpoint) {
    WebhookEndpointEntity entity = jpa.findById(endpoint.id()).orElseGet(WebhookEndpointEntity::new);
    entity.setId(endpoint.id());
    entity.setTenantId(endpoint.tenantId());
    entity.setTargetUrl(endpoint.targetUrl());
    entity.setSecret(endpoint.secret());
    entity.setPreviousSecret(endpoint.previousSecret());
    entity.setPreviousSecretUntil(endpoint.previousSecretUntil());
    entity.setEvents(endpoint.events());
    entity.setActive(endpoint.active());
    entity.setCreatedAt(entity.getCreatedAt() == null ? endpoint.createdAt() : entity.getCreatedAt());
    entity.setUpdatedAt(endpoint.updatedAt());
    return toDomain(jpa.save(entity));
  }

  @Override public Optional<WebhookEndpoint> findById(UUID id) { return jpa.findById(id).map(WebhookEndpointRepositoryImpl::toDomain); }
  @Override public List<WebhookEndpoint> findByTenantId(String tenantId) { return jpa.findByTenantIdOrderByCreatedAtAsc(tenantId).stream().map(WebhookEndpointRepositoryImpl::toDomain).toList(); }
  @Override public List<WebhookEndpoint> findActiveByTenantId(String tenantId) { return jpa.findByTenantIdAndActiveTrueOrderByCreatedAtAsc(tenantId).stream().map(WebhookEndpointRepositoryImpl::toDomain).toList(); }
  @Override public List<WebhookEndpoint> findAll() { return jpa.findAll().stream().map(WebhookEndpointRepositoryImpl::toDomain).toList(); }

  private static WebhookEndpoint toDomain(WebhookEndpointEntity e) {
    return new WebhookEndpoint(e.getId(), e.getTenantId(), e.getTargetUrl(), e.getSecret(),
        e.getPreviousSecret(), e.getPreviousSecretUntil(), e.getEvents(), e.isActive(),
        e.getCreatedAt(), e.getUpdatedAt());
  }
```
Remova `@Repository` das duas `*RepositoryImpl`: os beans são declarados na autoconfig. **Atenção**: sem `@Repository` a tradução de exceção (`InvalidDataAccessApiUsageException`) que `recusaReivindicarForaDeTransacao` espera deixa de acontecer. Mantenha `@Repository` nas duas `Impl` e registre-as na autoconfig por `@Import` (Step 5) — `@Import` de uma classe anotada com `@Repository` funciona e preserva o `PersistenceExceptionTranslationPostProcessor`.

- [ ] **Step 5: Flyway próprio e autoconfiguração**

`config/WebhookDeliveryFlywayConfiguration.java`:
```java
package com.barrier.webhookdelivery.config;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Flyway <b>próprio</b> da lib: schema {@code webhook_delivery}, histórico
 * {@code flyway_schema_history_webhook_delivery}, migrations em
 * {@code db/migration/webhook-delivery}.
 *
 * <p>Separado do Flyway do consumidor porque dois produtos com histórias de migração diferentes não
 * conseguem compartilhar uma sequência de versões — o Barrier está na V009 do serviço dele, o
 * gateway começa do zero, e a lib precisa ser a V1 para os dois.
 *
 * <p>O {@link BeanFactoryPostProcessor} faz o {@code entityManagerFactory} depender deste bean: sem
 * isso o Hibernate valida o schema ({@code ddl-auto=validate}) antes de a V1 rodar e a subida falha
 * com "tabela não existe". É o mesmo que o Boot faz para o Flyway dele via
 * {@code EntityManagerFactoryDependsOnPostProcessor}, escrito à mão porque esse pós-processador é
 * interno e mudou de pacote entre majors.
 */
@Configuration(proxyBeanMethods = false)
public class WebhookDeliveryFlywayConfiguration {

  static final String BEAN = "webhookDeliveryFlyway";

  @Bean(name = BEAN, initMethod = "migrate")
  public Flyway webhookDeliveryFlyway(DataSource dataSource, WebhookDeliveryProperties properties) {
    return Flyway.configure()
        .dataSource(dataSource)
        .schemas("webhook_delivery")
        .defaultSchema("webhook_delivery")
        .table("flyway_schema_history_webhook_delivery")
        .locations("classpath:db/migration/webhook-delivery")
        .baselineOnMigrate(properties.flyway().baselineOnMigrate())
        .baselineVersion(properties.flyway().baselineVersion())
        .load();
  }

  @Bean
  public static BeanFactoryPostProcessor entityManagerFactoryDependsOnWebhookDeliveryFlyway() {
    return new BeanFactoryPostProcessor() {
      @Override
      public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        for (String name : beanFactory.getBeanNamesForType(jakarta.persistence.EntityManagerFactory.class, true, false)) {
          BeanDefinition bd = beanFactory.getBeanDefinition(name);
          String[] atual = bd.getDependsOn();
          String[] novo = atual == null ? new String[] {BEAN} : java.util.Arrays.copyOf(atual, atual.length + 1);
          if (atual != null) novo[atual.length] = BEAN;
          bd.setDependsOn(novo);
        }
      }
    };
  }
}
```

`config/WebhookDeliveryAutoConfiguration.java` (versão desta task; cresce nas Tasks 8–9):
```java
package com.barrier.webhookdelivery.config;

import com.barrier.webhookdelivery.repository.DeliveryRepositoryImpl;
import com.barrier.webhookdelivery.repository.WebhookEndpointRepositoryImpl;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/**
 * Liga a lib no Spring do consumidor sem exigir {@code @ComponentScan} em pacote dela.
 *
 * <p>{@code @Import} explícito, um a um, em vez de scan: é a mesma regra do Barrier ("o que vem da
 * biblioteca é escolhido um a um, e fica legível que foi").
 *
 * <p>{@link AutoConfigurationPackage} registra {@code com.barrier.webhookdelivery.repository} para o
 * scan de entidades e de repositórios Spring Data do Boot. Consumidor que declara o próprio
 * {@code @EntityScan}/{@code @EnableJpaRepositories} precisa incluir esse pacote — está no README.
 */
@AutoConfiguration
@AutoConfigurationPackage(basePackages = "com.barrier.webhookdelivery.repository")
@EnableConfigurationProperties(WebhookDeliveryProperties.class)
@Import({
  WebhookDeliveryFlywayConfiguration.class,
  DeliveryRepositoryImpl.class,
  WebhookEndpointRepositoryImpl.class
})
public class WebhookDeliveryAutoConfiguration {}
```
As duas `*RepositoryImpl` precisam ser `public` para o `@Import` de fora do pacote (hoje são package-private no Barrier) — torne-as `public class`, mantendo o construtor package-private não; o construtor também precisa ser acessível ao Spring: deixe `public`.

`src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:
```
com.barrier.webhookdelivery.config.WebhookDeliveryAutoConfiguration
```

- [ ] **Step 6: Rodar os testes de integração (precisa de Docker)**

Run: `./mvnw -B -q test -Dtest='*IntegrationTest'`
Expected: `DeliveryOrderingIntegrationTest` (4), `DeliveryClaimExclusionIntegrationTest` (4), `WebhookEndpointRepositoryIntegrationTest` (2) verdes. Se o contexto falhar com "relation webhook_delivery.deliveries does not exist", o `dependsOn` do Step 5 não pegou o nome do bean do EMF — imprima `beanFactory.getBeanNamesForType(EntityManagerFactory.class, true, false)` e ajuste.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/repository src/main/java/com/barrier/webhookdelivery/config src/main/resources/META-INF src/test/java/com/barrier/webhookdelivery/repository
git commit -m "feat(repository): persistencia no schema proprio, flyway proprio e autoconfig

As tres travas do claimDue vem intactas do barrier, com os testes que
as provam. O Flyway da lib tem historico separado porque Barrier e
gateway tem historias de migracao diferentes; o pos-processador faz o
EntityManagerFactory esperar a V1 antes de validar o schema."
```

---

### Task 8: Cliente HTTP com headers configuráveis

**Files:**
- Create: `client/WebhookClient.java`, `client/WebhookRequest.java`, `client/WebhookSendResult.java`, `client/HttpWebhookClient.java`
- Modify: `config/WebhookDeliveryAutoConfiguration.java`
- Test: `src/test/java/com/barrier/webhookdelivery/client/HttpWebhookClientTest.java`

**Interfaces:**
- Produces: `record WebhookRequest(String url, String body, String eventId, String eventType, String signature, String previousSignature)`; `HttpWebhookClient(WebhookDeliveryProperties)`; `WebhookClient.send(WebhookRequest) -> WebhookSendResult`.

- [ ] **Step 1: Teste com `HttpServer` da JDK**

```java
package com.barrier.webhookdelivery.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HttpWebhookClientTest {

  static HttpServer server;
  static final Map<String, String> headers = new ConcurrentHashMap<>();
  static volatile int statusToReturn = 200;

  @BeforeAll
  static void sobe() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/hook", ex -> {
      headers.clear();
      ex.getRequestHeaders().forEach((k, v) -> headers.put(k, v.getFirst()));
      ex.getRequestBody().readAllBytes();
      ex.sendResponseHeaders(statusToReturn, -1);
      ex.close();
    });
    server.start();
  }

  @AfterAll
  static void desce() { server.stop(0); }

  private static HttpWebhookClient cliente() {
    return new HttpWebhookClient(new WebhookDeliveryProperties(
        0, null, 0, 0, null, Duration.ofSeconds(1), Duration.ofSeconds(1), null,
        new WebhookDeliveryProperties.Headers("X-Gateway"), null, null, null));
  }

  private String url() { return "http://localhost:" + server.getAddress().getPort() + "/hook"; }

  @Test
  void enviaOsQuatroHeadersComOPrefixoConfigurado() {
    statusToReturn = 200;
    WebhookSendResult r = cliente().send(new WebhookRequest(url(), "{}", "evt-1", "payment.completed", "t=1,v1=abc", "t=1,v1=old"));
    assertThat(r.success()).isTrue();
    assertThat(headers).containsEntry("X-gateway-signature", "t=1,v1=abc")
        .containsEntry("X-gateway-signature-previous", "t=1,v1=old")
        .containsEntry("X-gateway-event-id", "evt-1")
        .containsEntry("X-gateway-event-type", "payment.completed");
  }

  @Test
  void naoDoisXxViraFalhaSemLancar() {
    statusToReturn = 500;
    WebhookSendResult r = cliente().send(new WebhookRequest(url(), "{}", "evt-2", "x", "s", null));
    assertThat(r.success()).isFalse();
    assertThat(r.statusCode()).isEqualTo(500);
  }

  @Test
  void destinoForaDoArViraFalhaSemLancar() {
    WebhookSendResult r = cliente().send(new WebhookRequest("http://localhost:1/hook", "{}", "evt-3", "x", "s", null));
    assertThat(r.success()).isFalse();
    assertThat(r.statusCode()).isZero();
  }
}
```
Nota: o `HttpServer` da JDK normaliza nomes de header para `Xxx-xxx`; por isso as chaves no `containsEntry` estão em `X-gateway-...`. Se a asserção falhar por capitalização, compare com `entrySet()` case-insensitive em vez de mudar o cliente.

- [ ] **Step 2: Rodar e ver falhar** — `./mvnw -B -q test -Dtest=HttpWebhookClientTest`, erro de compilação.

- [ ] **Step 3: Copiar e ajustar**

- `WebhookClient` e `WebhookSendResult`: copie verbatim, pacote novo.
- `WebhookRequest`: record passa a `(String url, String body, String eventId, String eventType, String signature, String previousSignature)`; remova o construtor secundário; no javadoc, "X-Barrier-Signature" → "o header de assinatura".
- `HttpWebhookClient`: copie, remova `@Component` e os `@Value`; construtor `public HttpWebhookClient(WebhookDeliveryProperties properties)` lendo `properties.connectTimeout()`/`readTimeout()` e guardando `properties.headers()` em um campo `private final WebhookDeliveryProperties.Headers headers;`. Substitua as três constantes por chamadas `headers.eventId()`, `headers.signature()`, `headers.previousSignature()` e adicione `.header(headers.eventType(), request.eventType())`. O javadoc sobre timeouts fica.

- [ ] **Step 4: Registrar na autoconfig**

Em `WebhookDeliveryAutoConfiguration`, adicione dois `@Bean`:
```java
  @Bean
  @ConditionalOnMissingBean(WebhookClient.class)
  public WebhookClient webhookClient(WebhookDeliveryProperties properties) { return new HttpWebhookClient(properties); }

  @Bean
  public HmacSigner hmacSigner() { return new HmacSigner(); }
```
(`@ConditionalOnMissingBean` no cliente: teste do consumidor pode trocar por um fake sem `@MockitoBean`.)

- [ ] **Step 5: Rodar e ver passar** — `./mvnw -B -q test -Dtest=HttpWebhookClientTest`, verde.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery/client src/main/java/com/barrier/webhookdelivery/config src/test/java/com/barrier/webhookdelivery/client
git commit -m "feat(client): headers com prefixo configuravel e novo -Event-Type

X-Barrier vira propriedade porque o gateway assina como X-Gateway.
-Event-Type e aditivo: consumidor do Barrier que ignora headers
desconhecidos nao percebe."
```

---

### Task 9: Serviços — fan-out por endpoint, tentativa, scheduler; teste ponta a ponta

**Files:**
- Create: `service/WebhookEndpointService.java`, `service/WebhookDeliveryService.java`, `service/DeliveryRetryScheduler.java`
- Modify: `config/WebhookDeliveryAutoConfiguration.java`
- Test: `src/test/java/com/barrier/webhookdelivery/service/WebhookDeliveryIntegrationTest.java`, `service/WebhookEndpointServiceIntegrationTest.java`

**Interfaces:**
- Consumes: tudo das Tasks 2–8.
- Produces:
  ```java
  class WebhookEndpointService {
    WebhookEndpoint register(String tenantId, String targetUrl, List<String> events);
    /** Upsert do endpoint único do tenant (o mais antigo), events={"*"}; preserva segredo. Contrato do Barrier. */
    WebhookEndpoint registerSingle(String tenantId, String targetUrl);
    Optional<WebhookEndpoint> update(UUID id, String targetUrl, List<String> events);
    Optional<WebhookEndpoint> rotateSecret(UUID id);
    Optional<WebhookEndpoint> deactivate(UUID id);
    Optional<WebhookEndpoint> find(UUID id);
    List<WebhookEndpoint> listByTenant(String tenantId);
    List<WebhookEndpoint> listAll();
    Optional<SigningMaterial> resolveSigningMaterial(UUID endpointId);   // vazio se inativo/inexistente
  }
  class WebhookDeliveryService implements DeliveryIntake { IntakeResult accept(DeliveryRequest r); int retryDue(); }
  class DeliveryRetryScheduler { @Scheduled void retry(); }
  ```

- [ ] **Step 1: Teste ponta a ponta reescrito sobre `DeliveryRequest`**

```java
package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.client.HmacSigner;
import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import com.barrier.webhookdelivery.intake.DeliveryIntake;
import com.barrier.webhookdelivery.intake.DeliveryRequest;
import com.barrier.webhookdelivery.intake.IntakeResult;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Fluxo fim-a-fim sem Kafka: accept() registra, o scheduler entrega, o destino recebe assinado.
 * Requer Docker.
 */
@SpringBootTest(properties = {
    "webhook-delivery.retry-delay-ms=200",
    "webhook-delivery.headers.prefix=X-Test"
})
@Testcontainers
class WebhookDeliveryIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  record Recebido(String body, Map<String, String> headers) {}
  static final Map<String, List<Recebido>> porCaminho = new ConcurrentHashMap<>();
  static HttpServer server;

  @BeforeAll
  static void sobe() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    for (String caminho : List.of("/a", "/b")) {
      server.createContext(caminho, ex -> {
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> h = new ConcurrentHashMap<>();
        ex.getRequestHeaders().forEach((k, v) -> h.put(k.toLowerCase(), v.getFirst()));
        porCaminho.computeIfAbsent(caminho, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(new Recebido(body, h));
        ex.sendResponseHeaders(200, -1);
        ex.close();
      });
    }
    server.start();
  }

  @AfterAll static void desce() { server.stop(0); }

  @Autowired DeliveryIntake intake;
  @Autowired WebhookEndpointService endpoints;
  @Autowired HmacSigner signer;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void limpa() {
    jdbc.update("DELETE FROM webhook_delivery.deliveries");
    jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints");
    porCaminho.clear();
  }

  private String url(String caminho) { return "http://localhost:" + server.getAddress().getPort() + caminho; }

  @Test
  void entregaAssinadaAoEndpointInscritoENaoAoOutro() {
    WebhookEndpoint a = endpoints.register("t1", url("/a"), List.of("payment.*"));
    endpoints.register("t1", url("/b"), List.of("refund.*"));
    UUID eventId = UUID.randomUUID();

    IntakeResult r = intake.accept(new DeliveryRequest("t1", "payment.completed", eventId, "pay_1", "pay_1", "{\"id\":\"pay_1\"}", "corr-1"));

    assertThat(r).isEqualTo(new IntakeResult(1, 1));
    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(porCaminho.getOrDefault("/a", List.of())).hasSize(1));
    Recebido rec = porCaminho.get("/a").getFirst();
    assertThat(rec.body()).isEqualTo("{\"id\":\"pay_1\"}");
    assertThat(rec.headers()).containsEntry("x-test-event-id", eventId.toString()).containsEntry("x-test-event-type", "payment.completed");
    String assinatura = rec.headers().get("x-test-signature");
    long t = Long.parseLong(assinatura.substring(2, assinatura.indexOf(',')));
    assertThat(assinatura).isEqualTo(signer.sign(rec.body(), a.secret(), java.time.Instant.ofEpochSecond(t)));
    assertThat(porCaminho).doesNotContainKey("/b");
  }

  @Test
  void mesmoEventoDuasVezesEntregaUmaVezPorEndpoint() {
    endpoints.register("t1", url("/a"), List.of("*"));
    endpoints.register("t1", url("/b"), List.of("*"));
    UUID eventId = UUID.randomUUID();
    DeliveryRequest req = new DeliveryRequest("t1", "payment.completed", eventId, "pay_2", null, "{}", null);

    assertThat(intake.accept(req)).isEqualTo(new IntakeResult(2, 2));
    assertThat(intake.accept(req)).isEqualTo(new IntakeResult(2, 0));

    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      assertThat(porCaminho.getOrDefault("/a", List.of())).hasSize(1);
      assertThat(porCaminho.getOrDefault("/b", List.of())).hasSize(1);
    });
  }

  @Test
  void tenantSemEndpointNaoRegistraNada() {
    IntakeResult r = intake.accept(new DeliveryRequest("ninguem", "x.y", UUID.randomUUID(), "a", null, "{}", null));
    assertThat(r).isEqualTo(IntakeResult.NONE);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM webhook_delivery.deliveries", Integer.class)).isZero();
  }

  @Test
  void endpointDesativadoAposRegistroMataAEntrega() {
    WebhookEndpoint a = endpoints.register("t1", url("/a"), List.of("*"));
    jdbc.update("UPDATE webhook_delivery.webhook_endpoints SET active = false WHERE id = ?", a.id());
    // Registrado antes da desativação: simula a corrida real com uma linha já gravada.
    jdbc.update("""
        INSERT INTO webhook_delivery.deliveries (id, event_id, endpoint_id, event_type, aggregate_id, tenant_id, target_url, payload, status, attempts, next_attempt_at, created_at)
        VALUES (?, ?, ?, 'x', 'a', 't1', ?, '{}', 'PENDING', 0, now(), now())
        """, UUID.randomUUID(), UUID.randomUUID(), a.id(), url("/a"));

    Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        assertThat(jdbc.queryForObject("SELECT status FROM webhook_delivery.deliveries", String.class)).isEqualTo("DEAD"));
    assertThat(porCaminho).doesNotContainKey("/a");
  }
}
```

`WebhookEndpointServiceIntegrationTest.java`:
```java
package com.barrier.webhookdelivery.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.barrier.webhookdelivery.domain.WebhookEndpoint;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class WebhookEndpointServiceIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired WebhookEndpointService service;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach void limpa() { jdbc.update("DELETE FROM webhook_delivery.webhook_endpoints"); }

  /** O contrato do Barrier: PUT /v1/webhook-endpoints/{tenant} duas vezes = mesmo endpoint, mesmo segredo, URL nova. */
  @Test
  void registerSingleFazUpsertPreservandoSegredo() {
    WebhookEndpoint primeiro = service.registerSingle("t1", "https://acme/v1");
    WebhookEndpoint segundo = service.registerSingle("t1", "https://acme/v2");
    assertThat(segundo.id()).isEqualTo(primeiro.id());
    assertThat(segundo.secret()).isEqualTo(primeiro.secret());
    assertThat(segundo.targetUrl()).isEqualTo("https://acme/v2");
    assertThat(segundo.events()).isEqualTo(WebhookEndpoint.ALL_EVENTS);
    assertThat(service.listByTenant("t1")).hasSize(1);
  }

  @Test
  void rotacaoEDesativacaoPorId() {
    WebhookEndpoint e = service.register("t1", "https://acme/hook", List.of("*"));
    WebhookEndpoint r = service.rotateSecret(e.id()).orElseThrow();
    assertThat(r.secret()).isNotEqualTo(e.secret());
    assertThat(service.resolveSigningMaterial(e.id()).orElseThrow().previousSecret()).isEqualTo(e.secret());
    service.deactivate(e.id());
    assertThat(service.resolveSigningMaterial(e.id())).isEmpty();
  }
}
```

- [ ] **Step 2: Rodar e ver falhar** — `./mvnw -B -q test -Dtest='WebhookDeliveryIntegrationTest,WebhookEndpointServiceIntegrationTest'`, erro de compilação.

- [ ] **Step 3: `WebhookEndpointService`**

Copie `BARRIER/service/WebhookEndpointService.java`, pacote novo, remova `@Service`/`@Value`/`WebhookProperties`, e reescreva o corpo (o javadoc de `register` sobre preservar segredo e o de `rotateSecret` sobre a janela ficam):

```java
public class WebhookEndpointService {
  private static final Logger log = LoggerFactory.getLogger(WebhookEndpointService.class);
  private final WebhookEndpointRepository repository;
  private final Duration rotationOverlap;

  public WebhookEndpointService(WebhookEndpointRepository repository, WebhookDeliveryProperties properties) {
    this.repository = repository;
    this.rotationOverlap = properties.secretRotationOverlap();
  }

  @Transactional
  public WebhookEndpoint register(String tenantId, String targetUrl, List<String> events) {
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
```

- [ ] **Step 4: `WebhookDeliveryService`**

Copie `BARRIER/service/WebhookDeliveryService.java`, pacote novo, e:
1. Remova `@Service`, `@Value`, `ObjectMapper`, `EventEnvelope`, `WebhookProperties`; adicione `implements DeliveryIntake`. Construtor: `public WebhookDeliveryService(DeliveryRepository repository, WebhookEndpointService endpoints, WebhookClient client, HmacSigner signer, WebhookDeliveryProperties properties, TransactionTemplate transactionTemplate)`; `workers = properties.workers()`, `lease = properties.lease()`, guarde `properties` num campo.
2. Substitua `onEvent` e `partitionKeyDe` por:
```java
  @Override
  public IntakeResult accept(DeliveryRequest request) {
    return Correlation.run(properties.correlationMdcKey(), request.correlationId(), () -> registrar(request));
  }

  private IntakeResult registrar(DeliveryRequest request) {
    List<WebhookEndpoint> inscritos =
        endpoints.listByTenant(request.tenantId()).stream()
            .filter(WebhookEndpoint::active)
            .filter(e -> e.subscribedTo(request.eventType()))
            .toList();
    if (inscritos.isEmpty()) {
      // Não entregar é o desfecho correto — entregar no lugar errado é irreversível, e o fato
      // continua registrado no sistema de origem.
      log.warn("Tenant {} sem endpoint inscrito em {}; evento {} não entregue", request.tenantId(), request.eventType(), request.eventId());
      return IntakeResult.NONE;
    }
    int criadas = 0;
    for (WebhookEndpoint endpoint : inscritos) {
      try {
        repository.save(Delivery.create(request.eventId(), endpoint.id(), request.eventType(), request.aggregateId(),
            request.tenantId(), endpoint.targetUrl(), request.payload(), request.partitionKey()));
        criadas++;
      } catch (DataIntegrityViolationException e) {
        // (event_id, endpoint_id) já existe: repetição de quem chama, ou corrida entre réplicas.
        log.debug("Entrega do evento {} para o endpoint {} já registrada; ignorando", request.eventId(), endpoint.id());
      }
    }
    // A entrega NÃO acontece aqui, de propósito — ver o comentário original: quem entrega é o
    // retryDue(), pelo pool, fora da thread de quem chamou.
    return new IntakeResult(inscritos.size(), criadas);
  }
```
`Correlation.run` do Task 5 é `void`; faça uma sobrecarga `public static <T> T call(String mdcKey, String correlationId, Supplier<T> action)` em `Correlation` com o mesmo restaurar-o-anterior, e use `call` aqui.
3. Em `attempt(Delivery)`: substitua `SigningMaterial material = endpoints.resolveSigningMaterial(delivery.tenantId());` por
```java
    Optional<SigningMaterial> material = endpoints.resolveSigningMaterial(delivery.endpointId());
    if (material.isEmpty()) {
      delivery.markDead("endpoint desativado ou removido");
      repository.save(delivery);
      log.warn("Entrega {} encerrada: endpoint {} não está mais ativo", delivery.id(), delivery.endpointId());
      return;
    }
```
e use `material.get()` no resto. Na construção de `WebhookRequest`, inclua `delivery.eventType()` na posição nova.
4. `properties.maxAttempts()` e `properties.baseBackoff()` já batem com os nomes do record novo. O javadoc de `retryDue`, `comPermissao` e o comentário sobre o semáforo ficam verbatim.

- [ ] **Step 5: `DeliveryRetryScheduler` e autoconfig**

`DeliveryRetryScheduler` — copie, remova `@Component`, troque a expressão por `@Scheduled(fixedDelayString = "${webhook-delivery.retry-delay-ms:5000}")`.

Em `WebhookDeliveryAutoConfiguration` adicione:
```java
  @Bean public WebhookEndpointService webhookEndpointService(WebhookEndpointRepository r, WebhookDeliveryProperties p) { return new WebhookEndpointService(r, p); }

  @Bean
  public WebhookDeliveryService webhookDeliveryService(DeliveryRepository r, WebhookEndpointService e, WebhookClient c, HmacSigner s,
      WebhookDeliveryProperties p, PlatformTransactionManager tm) {
    return new WebhookDeliveryService(r, e, c, s, p, new TransactionTemplate(tm));
  }

  /** Requer @EnableScheduling no consumidor; desliga-se com webhook-delivery.scheduler.enabled=false (testes de claim). */
  @Bean
  @ConditionalOnProperty(prefix = "webhook-delivery.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
  public DeliveryRetryScheduler deliveryRetryScheduler(WebhookDeliveryService s) { return new DeliveryRetryScheduler(s); }
```
`@Transactional` nos services exige proxy: os beans criados por `@Bean` são proxiados pelo `TransactionInterceptor` do Boot normalmente (classe não-final, métodos públicos) — mantenha as classes não-`final`.

- [ ] **Step 6: Rodar toda a suíte**

Run: `./mvnw -B test`
Expected: todos verdes, incluindo os de Tasks 2–8.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/barrier/webhookdelivery src/test/java/com/barrier/webhookdelivery/service
git commit -m "feat(service): accept faz fan-out por endpoint inscrito; retryDue entrega

A idempotencia e por (evento, endpoint). Endpoint desativado entre o
registro e a tentativa mata a entrega em vez de queimar retries num
destino que o proprio tenant desligou. Tudo o que tinha comentario de
incidente no barrier (pool fora da thread de quem chama, semaforo como
teto real) veio intacto."
```

---

### Task 10: ArchUnit da lib

**Files:**
- Test: `src/test/java/com/barrier/webhookdelivery/architecture/ArchitectureTest.java`

- [ ] **Step 1: Escrever as regras**

```java
package com.barrier.webhookdelivery.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** As regras da spec §5. Cada uma com nome, para a falha dizer qual fronteira caiu. */
@AnalyzeClasses(packages = "com.barrier.webhookdelivery", importOptions = ImportOption.DoNotIncludeTests.class)
public class ArchitectureTest {

  /** Guarda contra passar vacuamente sobre zero classes (ASM que não lê o bytecode do JDK). */
  @ArchTest
  static void o_import_enxerga_as_classes(JavaClasses classes) {
    assertThat(classes.size()).isGreaterThan(20);
  }

  @ArchTest
  static final ArchRule dominio_nao_conhece_spring_nem_jpa =
      noClasses().that().resideInAPackage("..domain..")
          .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "jakarta.persistence..", "..repository..", "..service..", "..client..", "..config..");

  @ArchTest
  static final ArchRule client_nao_conhece_repository =
      noClasses().that().resideInAPackage("..client..").should().dependOnClassesThat().resideInAPackage("..repository..");

  @ArchTest
  static final ArchRule ninguem_importa_commons_nem_kafka =
      noClasses().should().dependOnClassesThat().resideInAnyPackage("com.barrier.commons..", "org.apache.kafka..", "org.springframework.kafka..");

  @ArchTest
  static final ArchRule entidades_jpa_sao_package_private =
      com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes().that().areAnnotatedWith(jakarta.persistence.Entity.class)
          .should().bePackagePrivate();
}
```

- [ ] **Step 2: Rodar** — `./mvnw -B -q test -Dtest=ArchitectureTest`. Expected: verde. Se `dominio_nao_conhece_spring` falhar, é porque alguma classe de `domain` importou `org.springframework` — corrija a classe, não a regra.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/barrier/webhookdelivery/architecture
git commit -m "test(arch): fronteiras da lib cobradas por teste

Dominio sem Spring/JPA, client sem repository, ninguem importa commons
nem kafka, entidade JPA package-private. Regra com nome para a falha
dizer qual fronteira caiu."
```

---

### Task 11: README, CI e publicação `0.1.0` no GitHub Packages

**Files:**
- Create: `README.md`, `.github/workflows/ci.yml`
- Modify: `pom.xml` (`distributionManagement`, `scm`)

- [ ] **Step 1: README**

```markdown
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

```java
@Autowired DeliveryIntake intake;
intake.accept(new DeliveryRequest(tenantId, "payment.completed", eventId, "pay_1", "pay_1", json, correlationId));
```

## Propriedades (`webhook-delivery.*`)

| propriedade | default | o quê |
|---|---|---|
| `workers` | 3 | entregas simultâneas (teto real; cabe no pool de conexões) |
| `lease` | PT2M | posse de uma entrega por um worker; maior que connect+read timeout |
| `retry-delay-ms` | 5000 | intervalo do scheduler |
| `max-attempts` | 5 | depois disso a entrega vira `DEAD` |
| `base-backoff` | PT30S | backoff exponencial, teto 64x |
| `connect-timeout` / `read-timeout` | PT2S / PT10S | |
| `secret-rotation-overlap` | PT24H | janela em que o segredo anterior ainda assina |
| `headers.prefix` | `X-Webhook` | `-Signature`, `-Signature-Previous`, `-Event-Id`, `-Event-Type` |
| `correlation-mdc-key` | `correlationId` | |
| `scheduler.enabled` | true | |
| `flyway.baseline-on-migrate` / `flyway.baseline-version` | false / 1 | para schema pré-existente |

## Assinatura

`<prefix>-Signature: t=<epoch-segundos>,v1=<hex HMAC-SHA256(secret, t + "." + body)>`.
Verifique com o `t=` do header, rejeite se for velho demais. Durante rotação, `-Signature-Previous`
traz a assinatura pelo segredo anterior.

## Persistência

Schema `webhook_delivery`, histórico Flyway `flyway_schema_history_webhook_delivery`, migrations
próprias. Consumidor com schema já existente (Barrier): mova as tabelas e use
`flyway.baseline-on-migrate=true`.

## Desenvolvimento

`./mvnw verify` (Testcontainers; precisa de Docker). Publicação: tag `vX.Y.Z` no `main`.
```

- [ ] **Step 2: `pom.xml` — publicação**

Adicione dentro de `<project>`:
```xml
    <scm>
        <url>https://github.com/leonardolermen/webhook-delivery</url>
    </scm>
    <distributionManagement>
        <repository>
            <id>github-webhook-delivery</id>
            <url>https://maven.pkg.github.com/leonardolermen/webhook-delivery</url>
        </repository>
    </distributionManagement>
```

- [ ] **Step 3: CI**

`.github/workflows/ci.yml`:
```yaml
name: CI

on:
  push:
    branches: [main]
    tags: ["v*"]
  pull_request:

concurrency:
  group: ci-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true

permissions:
  contents: read
  packages: write

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { java-version: "25", distribution: corretto, cache: maven }
      # Unidade + integração (Testcontainers; o runner tem Docker) + arquitetura.
      - run: ./mvnw -B verify
      - if: always()
        uses: actions/upload-artifact@v4
        with: { name: surefire-reports, path: "**/target/surefire-reports/**", retention-days: 7 }

  publish:
    # Só tag vX.Y.Z: a versão do pom tem de ser a da tag, sem -SNAPSHOT. O job falha se não bater,
    # de propósito — publicar 0.1.0-SNAPSHOT sob a tag v0.1.0 é o erro que ninguém percebe até o
    # consumidor resolver a dependência errada.
    if: startsWith(github.ref, 'refs/tags/v')
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: "25"
          distribution: corretto
          cache: maven
          server-id: github-webhook-delivery
          server-username: GITHUB_ACTOR
          server-password: GITHUB_TOKEN
      - name: versao do pom == tag
        run: |
          TAG="${GITHUB_REF#refs/tags/v}"
          POM=$(./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout)
          test "$TAG" = "$POM" || { echo "pom=$POM tag=$TAG"; exit 1; }
      - run: ./mvnw -B -DskipTests deploy
        env:
          GITHUB_ACTOR: ${{ github.actor }}
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 4: Verificar localmente e commitar**

Run: `./mvnw -B -q verify` — Expected: `BUILD SUCCESS`.

```bash
git add README.md .github pom.xml
git commit -m "ci: build com testcontainers e publicacao no github packages por tag

Publica so em tag vX.Y.Z e recusa se a versao do pom nao bater com a
tag: publicar SNAPSHOT sob tag de release e o erro que so aparece
quando o consumidor resolve a dependencia errada."
```

- [ ] **Step 5: Criar o repositório remoto, subir e publicar `0.1.0`**

**Pede confirmação do dono antes** (cria repo público e publica pacote):
```bash
gh repo create leonardolermen/webhook-delivery --public --source=. --push
```
Depois, com o dono de acordo:
```bash
./mvnw -q versions:set -DnewVersion=0.1.0 -DgenerateBackupPoms=false
git commit -am "release: 0.1.0"
git tag v0.1.0 && git push && git push --tags
./mvnw -q versions:set -DnewVersion=0.2.0-SNAPSHOT -DgenerateBackupPoms=false
git commit -am "build: proxima versao 0.2.0-SNAPSHOT" && git push
```
Expected: workflow `publish` verde; `https://github.com/leonardolermen/webhook-delivery/packages` lista `com.barrier.webhook-delivery 0.1.0`.

---

### Task 12: Barrier adota `webhook-delivery 0.1.0`

Trabalho **no repo `C:\Dev\barrier`**, em branch `feat/webhook-delivery-lib`. Critério da spec §6: a suíte que fica no `webhook-api` passa sem mudar asserção; `X-Barrier-Signature` continua o header.

**Files (todos sob `C:\Dev\barrier\services\webhook-api\`):**
- Modify: `pom.xml`, `src/main/resources/application.yml`, `src/main/java/com/barrier/webhook/WebhookApplication.java`, `controller/AssessmentCompletedListener.java`, `controller/WebhookEndpointController.java`, `controller/dto/WebhookEndpointResponse.java`, `controller/dto/WebhookEndpointSecretResponse.java`, `service/DeliveryReconciliationJob.java`, `config/WorkerPoolConfig.java`
- Delete: `domain/*`, `repository/*`, `service/WebhookDeliveryService.java`, `service/WebhookEndpointService.java`, `service/DeliveryRetryScheduler.java`, `client/*`, `config/WebhookProperties.java`, `config/GlobalTargetUrlReadinessGuard.java`, `config/WebhookSecretReadinessGuard.java`, e os testes correspondentes já portados (`DeliveryOrderingIntegrationTest`, `DeliveryClaimExclusionIntegrationTest`, `client/HmacSignerTest`, `domain/DeliveryPartitionKeyTest`, `config/*Guard*Test`)
- Create: `src/main/resources/db/migration/V009__move_para_webhook_delivery.sql`
- Also modify: `C:\Dev\barrier\pom.xml` (repositório do GitHub Packages)

- [ ] **Step 1: Dependência**

No `C:\Dev\barrier\pom.xml`, dentro de `<project>`:
```xml
    <repositories>
        <repository>
            <id>github-webhook-delivery</id>
            <url>https://maven.pkg.github.com/leonardolermen/webhook-delivery</url>
        </repository>
    </repositories>
```
No `services/webhook-api/pom.xml`:
```xml
        <dependency>
            <groupId>com.barrier</groupId>
            <artifactId>webhook-delivery</artifactId>
            <version>0.1.0</version>
        </dependency>
```
No CI do Barrier (`.github/workflows/ci.yml`), o `setup-java` do job `build` ganha `server-id: github-webhook-delivery`, `server-username: GITHUB_ACTOR`, `server-password: GITHUB_TOKEN`, e o step `mvnw verify` ganha `env: { GITHUB_ACTOR: ..., GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }} }` — o token do próprio workflow lê pacotes públicos da mesma conta.

- [ ] **Step 2: Migração V009**

`V009__move_para_webhook_delivery.sql`:
```sql
-- A máquina de entrega saiu deste serviço para a biblioteca webhook-delivery, que é dona do schema
-- webhook_delivery e do próprio histórico Flyway. Esta migração é a ÚNICA vez que este serviço toca
-- naquele schema: move o que existe para a forma da V1 da lib, e a partir daqui a lib migra sozinha
-- (flyway.baseline-on-migrate=true, baseline-version=1 no application.yml).
--
-- job_locks fica aqui: é lease de jobs deste serviço (reconciliação), não da entrega.

CREATE SCHEMA IF NOT EXISTS webhook_delivery;

ALTER TABLE webhook.webhook_endpoints SET SCHEMA webhook_delivery;
ALTER TABLE webhook.deliveries SET SCHEMA webhook_delivery;

-- Endpoints: PK deixa de ser o tenant. Cada linha existente vira o "endpoint único" do tenant,
-- inscrito em tudo — exatamente o comportamento que tinha.
ALTER TABLE webhook_delivery.webhook_endpoints DROP CONSTRAINT webhook_endpoints_pkey;
ALTER TABLE webhook_delivery.webhook_endpoints ADD COLUMN id UUID;
UPDATE webhook_delivery.webhook_endpoints SET id = gen_random_uuid();
ALTER TABLE webhook_delivery.webhook_endpoints ALTER COLUMN id SET NOT NULL;
ALTER TABLE webhook_delivery.webhook_endpoints ADD PRIMARY KEY (id);
ALTER TABLE webhook_delivery.webhook_endpoints ADD COLUMN events TEXT[] NOT NULL DEFAULT '{*}';
CREATE INDEX idx_webhook_endpoints_tenant_active ON webhook_delivery.webhook_endpoints (tenant_id, active);

-- Entregas: aggregate_id, event_type e endpoint_id. As existentes são todas de assessment.completed
-- (o outro tópico, risk_level_changed, nasceu depois da V008 e nunca teve entrega gravada com tipo)
-- e apontam para o endpoint único do seu tenant.
ALTER TABLE webhook_delivery.deliveries RENAME COLUMN assessment_id TO aggregate_id;
ALTER TABLE webhook_delivery.deliveries ADD COLUMN event_type VARCHAR(120) NOT NULL DEFAULT 'barrier.assessment.completed';
ALTER TABLE webhook_delivery.deliveries ALTER COLUMN event_type DROP DEFAULT;
ALTER TABLE webhook_delivery.deliveries ADD COLUMN endpoint_id UUID;
UPDATE webhook_delivery.deliveries d
   SET endpoint_id = e.id
  FROM webhook_delivery.webhook_endpoints e
 WHERE e.tenant_id = d.tenant_id;
-- Entrega sem tenant ou de tenant sem endpoint (anteriores à V004): não há para onde entregar e
-- nunca houve; recebem um id sintético para satisfazer o NOT NULL sem inventar destino.
UPDATE webhook_delivery.deliveries SET endpoint_id = '00000000-0000-0000-0000-000000000000' WHERE endpoint_id IS NULL;
UPDATE webhook_delivery.deliveries SET tenant_id = 'desconhecido' WHERE tenant_id IS NULL;
ALTER TABLE webhook_delivery.deliveries ALTER COLUMN endpoint_id SET NOT NULL;
ALTER TABLE webhook_delivery.deliveries ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE webhook_delivery.deliveries DROP CONSTRAINT deliveries_event_id_key;
ALTER TABLE webhook_delivery.deliveries ADD CONSTRAINT uq_deliveries_event_endpoint UNIQUE (event_id, endpoint_id);
CREATE INDEX idx_deliveries_event ON webhook_delivery.deliveries (event_id);
```
Verifique o nome real da constraint UNIQUE de `event_id` com `\d webhook.deliveries` num banco migrado até V008 (default do Postgres: `deliveries_event_id_key`).

- [ ] **Step 3: `application.yml`**

Remova o bloco `barrier.webhook.*` de entrega e adicione:
```yaml
webhook-delivery:
  workers: ${WEBHOOK_WORKERS:3}
  headers:
    prefix: X-Barrier
  correlation-mdc-key: correlationId
  flyway:
    baseline-on-migrate: true
    baseline-version: "1"
```
Mantenha `barrier.webhook.reconciliation.*` (é do job que fica).

- [ ] **Step 4: Código que fica**

- `WebhookApplication`: remova `@EnableConfigurationProperties(WebhookProperties.class)`. Mantém `@EnableScheduling` e o scan em `com.barrier.webhook`.
- `AssessmentCompletedListener.onMessage`:
```java
    EventEnvelope envelope = parse(message);
    Map<String, Object> data = payload(envelope.payload());
    String tenantId = str(data.get("tenantId"));
    String subjectId = str(data.get("subjectId"));
    Correlation.run(envelope.correlationId(), () -> intake.accept(new DeliveryRequest(
        tenantId, envelope.type(), envelope.eventId(), envelope.assessmentId(),
        subjectId, envelope.payload(), envelope.correlationId())));
```
com `DeliveryIntake intake` injetado no lugar de `WebhookDeliveryService`, `payload(...)` sendo o antigo `extractTenantId` devolvendo o `Map` (mesmo tratamento de `MalformedEventException`), e `str(Object o) { return o == null ? null : o.toString(); }`. Tenant nulo: `DeliveryRequest` lança `IllegalArgumentException` — envolva em `MalformedEventException("Evento sem tenantId")` para ir à DLT como antes.
- `DeliveryReconciliationJob.recuperar`: mesma troca — monta `DeliveryRequest` e chama `intake.accept`; `repository.existsByEventId` vem de `com.barrier.webhookdelivery.repository.DeliveryRepository`.
- `WebhookEndpointController`: `service.register(tenantId, url)` → `service.registerSingle(tenantId, url)`; `rotateSecret(tenantId)`, `deactivate(tenantId)`, `find(tenantId)` → resolva o endpoint único por `service.listByTenant(tenantId).stream().findFirst()` e chame a versão por `id`; `list()` → `service.listAll()`. Os DTOs continuam expondo `tenantId` (lido de `endpoint.tenantId()`), então o JSON não muda.
- `WorkerPoolConfig`: `@Value("${webhook-delivery.workers:3}")` e a string do guard `"webhook-delivery.workers"`.
- Apague os arquivos listados em *Delete*. `GlobalTargetUrlReadinessGuard` e `WebhookSecretReadinessGuard` perdem o objeto (não há mais global); se `WebhookLoadTest` ou `WebhookEndpointApiIntegrationTest` dependiam de `barrier.webhook.target-url`, substitua por `endpointService.registerSingle("default", url)` no setup do teste — isso é mudança de setup, não de asserção.

- [ ] **Step 5: Rodar a suíte do Barrier**

Run: `cd /c/Dev/barrier && ./mvnw -B -pl services/webhook-api -am verify`
Expected: verde. Confirme que `WebhookEndpointApiIntegrationTest` e `AssessmentCompletedListenerTest` passaram **sem alteração de asserção** (`git diff --stat` nesses arquivos deve mostrar só imports/setup).

- [ ] **Step 6: Commit e PR**

```bash
git add -A
git commit -m "refactor(webhook-api): entrega de webhooks vem da lib webhook-delivery 0.1.0

A maquina de entrega (HMAC, rotacao, tres travas do claim, lease,
backoff) saiu para com.barrier:webhook-delivery, que o payment-gateway
tambem consome. Aqui ficam Kafka, reconciliacao pelo topico e a API
administrativa, que chama registerSingle para manter o contrato de
PUT /v1/webhook-endpoints/{tenantId} intocado. V009 move as tabelas
para o schema da lib e ela baseia o historico em 1.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
Abra o PR com `gh pr create` e deixe o CI do Barrier confirmar.

---

## Self-review

**Cobertura da spec.** §1 (repo próprio, coordenadas, Packages) → Tasks 1, 11. §2 (o que vai/fica) → Tasks 2–9 e 12. §3.1 entrada → Task 5, 9. §3.2 N endpoints/filtro/`registerSingle` → Tasks 4, 7, 9. §3.3 nomes → Tasks 6, 8. §3.4 Flyway próprio + V009 → Tasks 6, 7, 12. §4 (o que não muda) → cópias verbatim com comentário, Tasks 2, 7, 9. §5 ArchUnit → Task 10. §6 ordem e critério → Tasks 11–12.

**Placeholders.** Nenhum "TBD". Os pontos onde o executor precisa verificar algo no ambiente (nome do bean do EMF, nome da constraint UNIQUE, capitalização do `HttpServer`) dizem exatamente o que verificar e como.

**Consistência de tipos.** `Delivery.create(eventId, endpointId, eventType, aggregateId, tenantId, targetUrl, payload, partitionKey)` é a assinatura usada em Tasks 3, 7 (mapper) e 9. `WebhookRequest(url, body, eventId, eventType, signature, previousSignature)` em Tasks 8 e 9. `IntakeResult(endpointsMatched, deliveriesCreated)` em Tasks 5 e 9 — os testes de Task 9 usam `new IntakeResult(1, 1)`, `(2, 2)`, `(2, 0)` nessa ordem. `Correlation.run(mdcKey, id, Runnable)` + `call(mdcKey, id, Supplier)` em Tasks 5 e 9. `resolveSigningMaterial(UUID) -> Optional<SigningMaterial>` em Tasks 9 (service e teste).
