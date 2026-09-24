package com.barrier.webhookdelivery.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class EventTypeMatcherTest {

  @Test
  void curingaSozinhoCasaComTudo() {
    assertThat(EventTypeMatcher.matches(List.of("*"), "payment.completed")).isTrue();
  }

  @Test
  void igualdadeExata() {
    assertThat(EventTypeMatcher.matches(List.of("payment.completed"), "payment.completed")).isTrue();
    assertThat(EventTypeMatcher.matches(List.of("payment.completed"), "payment.pending")).isFalse();
  }

  @Test
  void prefixoComCuringaFinal() {
    assertThat(EventTypeMatcher.matches(List.of("payment.*"), "payment.completed")).isTrue();
    assertThat(EventTypeMatcher.matches(List.of("payment.*"), "refund.completed")).isFalse();
  }

  /** Só curinga FINAL: "*.completed" e "pay*ment" não são suportados, de propósito (sem glob). */
  @Test
  void curingaNoMeioOuNoInicioNaoCasa() {
    assertThat(EventTypeMatcher.matches(List.of("*.completed"), "payment.completed")).isFalse();
    assertThat(EventTypeMatcher.matches(List.of("pay*ment"), "payment")).isFalse();
  }

  @Test
  void listaVaziaOuNulaNaoCasaComNada() {
    assertThat(EventTypeMatcher.matches(List.of(), "payment.completed")).isFalse();
    assertThat(EventTypeMatcher.matches(null, "payment.completed")).isFalse();
  }
}
