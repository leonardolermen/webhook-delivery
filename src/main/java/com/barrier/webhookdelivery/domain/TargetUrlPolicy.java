package com.barrier.webhookdelivery.domain;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

/**
 * Política de saída para o destino de um webhook: recusa rede interna.
 *
 * <p>O tenant controla a URL de callback, e o servidor faz POST nela com documento, nome e veredito
 * do cliente final no corpo. Exigir TLS não impede que um tenant aponte o callback para um serviço
 * interno alcançável pelo pod (metadata da nuvem, admin de outro serviço) — SSRF. A política olha o
 * <b>endereço efetivamente acessado</b>: literal ou resolvido por DNS, todos os endereços do host
 * precisam ser públicos. Vale no registro e, de novo, em cada envio, porque o DNS do parceiro pode
 * mudar entre um e outro.
 *
 * <p>{@code allowPrivate=true} desliga a política inteira (inclusive a resolução): é o modo de
 * desenvolvimento, para destino em {@code localhost}. Nunca é o padrão.
 */
public final class TargetUrlPolicy {

  private final boolean allowPrivate;

  public TargetUrlPolicy(boolean allowPrivate) {
    this.allowPrivate = allowPrivate;
  }

  /** @throws IllegalArgumentException se o destino aponta para rede interna ou o host não resolve */
  public void check(String targetUrl) {
    if (allowPrivate) {
      return;
    }
    String host;
    try {
      host = new URI(targetUrl.trim()).getHost();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("targetUrl inválida: " + targetUrl, e);
    }
    if (host == null) {
      throw new IllegalArgumentException("targetUrl sem host: " + targetUrl);
    }
    // URI devolve IPv6 literal entre colchetes; InetAddress não os aceita.
    String semColchetes = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
    InetAddress[] enderecos;
    try {
      enderecos = InetAddress.getAllByName(semColchetes);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("targetUrl com host que não resolve: " + host, e);
    }
    for (InetAddress endereco : enderecos) {
      if (interno(endereco)) {
        throw new IllegalArgumentException(
            "targetUrl aponta para rede interna (" + endereco.getHostAddress() + "): " + targetUrl);
      }
    }
  }

  private static boolean interno(InetAddress a) {
    return a.isLoopbackAddress()
        || a.isAnyLocalAddress()
        || a.isLinkLocalAddress()
        || a.isSiteLocalAddress()
        || a.isMulticastAddress()
        || uniqueLocalIpv6(a);
  }

  /** fc00::/7 (ULA): o equivalente IPv6 do 10/8, que {@code isSiteLocalAddress} não cobre. */
  private static boolean uniqueLocalIpv6(InetAddress a) {
    byte[] b = a.getAddress();
    return b.length == 16 && (b[0] & 0xFE) == 0xFC;
  }
}
