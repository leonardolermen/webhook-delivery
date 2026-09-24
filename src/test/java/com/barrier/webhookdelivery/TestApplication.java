package com.barrier.webhookdelivery;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Aplicação mínima só para os testes de integração: a lib não tem main. Escaneia apenas este
 * pacote raiz de teste — as classes da lib entram pela autoconfiguração, exatamente como num
 * consumidor real. Se a autoconfig deixar de registrar algo, é aqui que o teste quebra.
 */
@SpringBootApplication(scanBasePackages = "com.barrier.webhookdelivery.testapp")
@EnableScheduling
public class TestApplication {}
