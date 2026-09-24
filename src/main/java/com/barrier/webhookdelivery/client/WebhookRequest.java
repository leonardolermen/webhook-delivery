package com.barrier.webhookdelivery.client;

/**
 * Requisição de entrega de webhook.
 *
 * @param url endpoint do cliente
 * @param body corpo (JSON do resultado da avaliação)
 * @param eventId id do evento (header de idempotência para o cliente)
 * @param eventType tipo do evento
 * @param signature assinatura HMAC do corpo com o segredo vigente do tenant
 * @param previousSignature assinatura pelo segredo anterior durante a janela de rotação;
 *     {@code null} fora dela. Vai em header próprio em vez de mudar o formato do header principal:
 *     cliente que já verifica o header de assinatura não precisa saber que existe rotação
 */
public record WebhookRequest(
    String url, String body, String eventId, String eventType, String signature, String previousSignature) {}
