package com.barrier.webhookdelivery.config;

import java.util.Arrays;
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
 * {@code flyway_schema_history_webhook_delivery}, migrations em {@code db/webhook-delivery}.
 *
 * <p>Separado do Flyway do consumidor porque dois produtos com histórias de migração diferentes não
 * conseguem compartilhar uma sequência de versões — o Barrier está na V009 do serviço dele, o
 * gateway começa do zero, e a lib precisa ser a V1 para os dois.
 *
 * <p>O bean é um {@link WebhookDeliveryMigrations}, nunca um {@link Flyway}: ver a classe — um
 * {@code Flyway} no contexto desliga o Flyway do consumidor.
 *
 * <p>A localização fica <b>fora</b> de {@code db/migration}: o Flyway varre locations
 * recursivamente, e o {@code classpath:db/migration} default do consumidor enxergava
 * {@code db/migration/webhook-delivery/V1__inicial.sql} dentro do jar da lib — versão 1 duplicada
 * com a V1/V001 do consumidor, e a subida falha em {@code validate}.
 *
 * <p>O {@link BeanFactoryPostProcessor} faz duas amarrações de ordem:
 *
 * <ul>
 *   <li>{@code entityManagerFactory} depende das migrations da lib: sem isso o Hibernate valida o
 *       schema ({@code ddl-auto=validate}) antes de a V1 rodar e a subida falha com "tabela não
 *       existe". É o que o Boot faz para o Flyway dele via
 *       {@code EntityManagerFactoryDependsOnPostProcessor}, escrito à mão porque esse
 *       pós-processador é interno e mudou de pacote entre majors.
 *   <li>as migrations da lib dependem do {@code flywayInitializer} do consumidor, quando ele existe:
 *       a V009 do Barrier <b>move</b> as tabelas dele para {@code webhook_delivery}, e precisa rodar
 *       antes da V1/baseline da lib, senão a lib cria tabelas vazias e a V009 colide com elas.
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class WebhookDeliveryFlywayConfiguration {

  static final String BEAN = "webhookDeliveryMigrations";

  /** Nome do bean que o {@code FlywayAutoConfiguration} do Boot registra para rodar o migrate. */
  static final String FLYWAY_DO_CONSUMIDOR = "flywayInitializer";

  @Bean(name = BEAN, initMethod = "migrate")
  public WebhookDeliveryMigrations webhookDeliveryMigrations(DataSource dataSource, WebhookDeliveryProperties properties) {
    return new WebhookDeliveryMigrations(
        Flyway.configure()
            .dataSource(dataSource)
            .schemas("webhook_delivery")
            .defaultSchema("webhook_delivery")
            .table("flyway_schema_history_webhook_delivery")
            .locations("classpath:db/webhook-delivery")
            .baselineOnMigrate(properties.flyway().baselineOnMigrate())
            .baselineVersion(properties.flyway().baselineVersion())
            .load());
  }

  @Bean
  public static BeanFactoryPostProcessor webhookDeliveryMigrationsOrdering() {
    return new BeanFactoryPostProcessor() {
      @Override
      public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        for (String name : beanFactory.getBeanNamesForType(jakarta.persistence.EntityManagerFactory.class, true, false)) {
          adicionaDependencia(beanFactory.getBeanDefinition(name), BEAN);
        }
        if (beanFactory.containsBeanDefinition(FLYWAY_DO_CONSUMIDOR) && beanFactory.containsBeanDefinition(BEAN)) {
          adicionaDependencia(beanFactory.getBeanDefinition(BEAN), FLYWAY_DO_CONSUMIDOR);
        }
      }
    };
  }

  private static void adicionaDependencia(BeanDefinition bd, String dependencia) {
    String[] atual = bd.getDependsOn();
    if (atual == null) {
      bd.setDependsOn(dependencia);
      return;
    }
    if (Arrays.asList(atual).contains(dependencia)) return;
    String[] novo = Arrays.copyOf(atual, atual.length + 1);
    novo[atual.length] = dependencia;
    bd.setDependsOn(novo);
  }
}
