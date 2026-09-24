package com.barrier.webhookdelivery.intake;

/**
 * @param endpointsMatched endpoints ativos do tenant inscritos no evento
 * @param deliveriesCreated entregas efetivamente registradas (menor que matched quando o evento já
 *     tinha entrega para algum endpoint — idempotência)
 */
public record IntakeResult(int endpointsMatched, int deliveriesCreated) {
  public static final IntakeResult NONE = new IntakeResult(0, 0);
}
