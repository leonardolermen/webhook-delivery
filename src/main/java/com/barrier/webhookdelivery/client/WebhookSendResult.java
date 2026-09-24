package com.barrier.webhookdelivery.client;

/** Resultado de uma tentativa de entrega. */
public record WebhookSendResult(boolean success, int statusCode, String detail) {

  public static WebhookSendResult ok(int statusCode) {
    return new WebhookSendResult(true, statusCode, "ok");
  }

  public static WebhookSendResult failure(int statusCode, String detail) {
    return new WebhookSendResult(false, statusCode, detail);
  }
}
