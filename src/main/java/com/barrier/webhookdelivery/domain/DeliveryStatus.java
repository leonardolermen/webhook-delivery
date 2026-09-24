package com.barrier.webhookdelivery.domain;

/** Estado do ciclo de vida de uma entrega de webhook. */
public enum DeliveryStatus {
  /** Criada, aguardando (primeira) tentativa. */
  PENDING,
  /** Entregue com sucesso (2xx). */
  DELIVERED,
  /** Falhou, com nova tentativa agendada. */
  FAILED,
  /** Esgotou as tentativas; não será mais tentada. */
  DEAD
}
