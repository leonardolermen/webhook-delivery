package com.barrier.webhookdelivery.observability;

import java.util.function.Supplier;
import org.slf4j.MDC;

/**
 * Cópia enxuta do {@code Correlation} do Barrier: a lib não pode depender de {@code commons}
 * (que puxa Kafka), e a chave do MDC é do consumidor — o Barrier usa {@code correlationId},
 * outro produto pode usar outra.
 *
 * <p>Restaurar (em vez de limpar) importa porque o processamento assíncrono roda numa thread de
 * pool reaproveitada: deixar o id de uma avaliação pendurado contaminaria a próxima.
 */
public final class Correlation {

  private Correlation() {}

  /** Id do MDC com a chave fornecida; {@code null} quando não há valor. */
  public static String current(String mdcKey) {
    return MDC.get(mdcKey);
  }

  /**
   * Executa com o id de correlação no MDC e restaura o estado anterior ao final.
   */
  public static void run(String mdcKey, String correlationId, Runnable action) {
    call(mdcKey, correlationId, () -> {
      action.run();
      return null;
    });
  }

  /**
   * Executa com o id de correlação no MDC e restaura o estado anterior ao final,
   * retornando o valor do supplier.
   */
  public static <T> T call(String mdcKey, String correlationId, Supplier<T> action) {
    String previous = MDC.get(mdcKey);
    if (correlationId != null && !correlationId.isBlank()) {
      MDC.put(mdcKey, correlationId);
    }
    try {
      return action.get();
    } finally {
      if (previous == null) {
        MDC.remove(mdcKey);
      } else {
        MDC.put(mdcKey, previous);
      }
    }
  }
}
