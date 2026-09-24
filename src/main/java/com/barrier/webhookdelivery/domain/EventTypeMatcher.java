package com.barrier.webhookdelivery.domain;

import java.util.List;

/**
 * Decide se um endpoint inscrito em {@code subscribed} recebe {@code eventType}.
 *
 * <p>Três formas e só elas: {@code *} (tudo), igualdade exata e prefixo com curinga <b>final</b>
 * ({@code payment.*}). Glob geral foi rejeitado: cada forma extra é um jeito a mais de um parceiro
 * achar que está inscrito e não estar, e o sintoma é "não recebi o webhook" — o mais caro de
 * investigar.
 */
public final class EventTypeMatcher {

  private EventTypeMatcher() {}

  public static boolean matches(List<String> subscribed, String eventType) {
    if (subscribed == null || eventType == null) {
      return false;
    }
    for (String pattern : subscribed) {
      if (pattern == null) {
        continue;
      }
      if (pattern.equals("*") || pattern.equals(eventType)) {
        return true;
      }
      if (pattern.endsWith(".*")
          && pattern.indexOf('*') == pattern.length() - 1
          && eventType.startsWith(pattern.substring(0, pattern.length() - 1))) {
        return true;
      }
    }
    return false;
  }
}
