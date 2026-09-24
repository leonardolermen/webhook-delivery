package com.barrier.webhookdelivery.config;

import com.barrier.webhookdelivery.client.HttpWebhookClient;
import com.barrier.webhookdelivery.client.WebhookClient;
import com.barrier.webhookdelivery.client.HmacSigner;
import com.barrier.webhookdelivery.repository.DeliveryRepositoryImpl;
import com.barrier.webhookdelivery.repository.WebhookEndpointRepositoryImpl;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
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
public class WebhookDeliveryAutoConfiguration {
  @Bean
  @ConditionalOnMissingBean(WebhookClient.class)
  public WebhookClient webhookClient(WebhookDeliveryProperties properties) { return new HttpWebhookClient(properties); }

  @Bean
  public HmacSigner hmacSigner() { return new HmacSigner(); }
}
