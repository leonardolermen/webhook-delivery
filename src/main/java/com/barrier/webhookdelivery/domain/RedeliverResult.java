package com.barrier.webhookdelivery.domain;

/** Desfecho de um pedido de reentrega manual. */
public enum RedeliverResult {
  /** Voltou a PENDING; o próximo ciclo do {@code retryDue} a tenta. */
  SCHEDULED,
  /** Não existe, ou é de outro tenant: os dois casos respondem igual, de propósito. */
  NOT_FOUND,
  /** Existe, mas não está DEAD/FAILED livre, ou o endpoint não está mais ativo. */
  NOT_REDELIVERABLE
}
