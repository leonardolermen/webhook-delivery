package com.barrier.webhookdelivery.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * O Flyway da lib convive com o do consumidor, em vez de desligá-lo.
 *
 * <p>Regressão do defeito em que a lib expunha um bean {@code Flyway}: o
 * {@code @ConditionalOnMissingBean(Flyway.class)} do Boot recuava e o {@code db/migration} do
 * consumidor nunca rodava — no Barrier, 8 migrations viraram 1. O contexto de teste tem a própria
 * {@code db/migration/V1__consumidor.sql}, como um consumidor real.
 */
@SpringBootTest(classes = com.barrier.webhookdelivery.testapp.TestApplication.class,
    properties = "webhook-delivery.scheduler.enabled=false")
@Testcontainers
class ConsumerFlywayCoexistenceIntegrationTest {

  @Container @ServiceConnection
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  @Autowired JdbcTemplate jdbc;

  @Test
  void asMigrationsDoConsumidorEDaLibRodamAsDuas() {
    assertThat(jdbc.queryForObject(
        "SELECT count(*) FROM public.flyway_schema_history WHERE success AND version = '1'", Integer.class))
        .isEqualTo(1);
    assertThat(jdbc.queryForObject(
        "SELECT count(*) FROM webhook_delivery.flyway_schema_history_webhook_delivery WHERE success AND version = '1'",
        Integer.class))
        .isEqualTo(1);
    assertThat(jdbc.queryForObject("SELECT to_regclass('public.consumidor_marcador') IS NOT NULL", Boolean.class))
        .isTrue();
  }

  @Test
  void oConsumidorMigraAntesDaLib() {
    // A V009 do Barrier move as tabelas dele para webhook_delivery e precisa vir antes da V1/baseline
    // da lib; installed_on é o relógio do banco na aplicação de cada migration.
    Timestamp consumidor = jdbc.queryForObject(
        "SELECT installed_on FROM public.flyway_schema_history WHERE version = '1'", Timestamp.class);
    Timestamp lib = jdbc.queryForObject(
        "SELECT installed_on FROM webhook_delivery.flyway_schema_history_webhook_delivery WHERE version = '1'",
        Timestamp.class);
    assertThat(consumidor).isBeforeOrEqualTo(lib);
  }
}
