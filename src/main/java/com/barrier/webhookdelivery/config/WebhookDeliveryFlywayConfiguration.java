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
