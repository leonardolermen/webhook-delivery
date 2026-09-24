package com.barrier.webhookdelivery.client;

import com.barrier.webhookdelivery.config.WebhookDeliveryProperties;
import com.barrier.webhookdelivery.domain.TargetUrlPolicy;
import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Envio via {@link RestClient}; qualquer erro/HTTP não-2xx vira uma falha (para retry).
 *
 * <p>Os timeouts não são detalhe de afinação: este cliente chama um endpoint <b>de terceiro</b> na
 * thread do listener do Kafka. Com {@code RestClient.create()} — sem timeout algum, que era o
 * estado anterior — um cliente que aceita a conexão e não responde parava o consumo da partição
 * inteira, e com ela a entrega de todos os outros tenants.
 */
public class HttpWebhookClient implements WebhookClient {

  private final RestClient restClient;
  private final WebhookDeliveryProperties.Headers headers;
  private final TargetUrlPolicy targetPolicy;

  public HttpWebhookClient(WebhookDeliveryProperties properties) {
    Duration connectTimeout = properties.connectTimeout();
    Duration readTimeout = properties.readTimeout();
    this.headers = properties.headers();
    this.targetPolicy = new TargetUrlPolicy(properties.allowPrivateTargets());

    // Connect timeout vive no HttpClient da JDK, não no request factory.
    HttpClient httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
    factory.setReadTimeout(readTimeout);
    this.restClient = RestClient.builder().requestFactory(factory).build();
  }

  @Override
  public WebhookSendResult send(WebhookRequest request) {
    try {
      // De novo aqui, e não só no registro: a URL da entrega foi validada quando o endpoint foi
      // registrado, e o DNS do parceiro pode ter passado a apontar para dentro desde então.
      targetPolicy.check(request.url());
      var spec =
          restClient
              .post()
              .uri(request.url())
              .contentType(MediaType.APPLICATION_JSON)
              .header(headers.eventId(), request.eventId())
              .header(headers.signature(), request.signature())
              .header(headers.eventType(), request.eventType());
      if (request.previousSignature() != null) {
        spec = spec.header(headers.previousSignature(), request.previousSignature());
      }
      var response = spec.body(request.body()).retrieve().toBodilessEntity();
      int status = response.getStatusCode().value();
      return response.getStatusCode().is2xxSuccessful()
          ? WebhookSendResult.ok(status)
          : WebhookSendResult.failure(status, "HTTP " + status);
    } catch (org.springframework.web.client.RestClientResponseException e) {
      return WebhookSendResult.failure(e.getStatusCode().value(), e.getMessage());
    } catch (RuntimeException e) {
      return WebhookSendResult.failure(0, e.getMessage());
    }
  }
}
