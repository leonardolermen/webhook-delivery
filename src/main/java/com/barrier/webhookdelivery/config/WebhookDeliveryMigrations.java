package com.barrier.webhookdelivery.config;

import org.flywaydb.core.Flyway;

/**
 * As migrations da lib, embrulhadas num tipo da lib.
 *
 * <p>Existe para a lib <b>não</b> expor um bean do tipo {@link Flyway}. O
 * {@code FlywayAutoConfiguration} do Boot 4.0.7 é {@code @ConditionalOnMissingBean(Flyway.class)}:
 * com um {@code Flyway} da lib no contexto, o do consumidor recuava em silêncio e o
 * {@code db/migration} dele nunca rodava. Evidência: o surefire do Barrier passou de "applied 8
 * migrations to schema webhook" para só "applied 1 migration to schema webhook_delivery" depois de
 * adotar a lib.
 */
public final class WebhookDeliveryMigrations {

  private final Flyway flyway;

  WebhookDeliveryMigrations(Flyway flyway) {
    this.flyway = flyway;
  }

  public void migrate() {
    flyway.migrate();
  }
}
