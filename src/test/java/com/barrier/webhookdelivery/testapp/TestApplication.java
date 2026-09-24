package com.barrier.webhookdelivery.testapp;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Aplicação mínima só para os testes de integração: a lib não tem main. Escaneia apenas este
 * pacote raiz de teste — as classes da lib entram pela autoconfiguração, exatamente como num
 * consumidor real. Se a autoconfig deixar de registrar algo, é aqui que o teste quebra.
 *
 * <p>Mora em {@code com.barrier.webhookdelivery.testapp} e não em {@code com.barrier.webhookdelivery}
 * (o pacote raiz da própria lib) de propósito: {@code @AutoConfigurationPackage} registra o pacote
 * da classe anotada com {@code @SpringBootApplication} para o scan de repositórios/entidades do
 * Boot — não o {@code scanBasePackages} do component scan, que é outro mecanismo. Com
 * {@code TestApplication} no pacote raiz, esse registro cobria {@code
 * com.barrier.webhookdelivery.repository} por ser subpacote, e colidia com o {@code
 * @AutoConfigurationPackage(basePackages = "com.barrier.webhookdelivery.repository")} desta lib: os
 * dois escaneavam a mesma interface e o Spring recusava o bean duplicado
 * ({@code BeanDefinitionOverrideException} em {@code DeliveryJpaRepository}). Como
 * {@code TestApplication} deixa de ser ancestral do pacote dos testes, cada teste de integração
 * aqui precisa de {@code @SpringBootTest(classes = TestApplication.class)} — a detecção automática
 * não alcança mais um pacote irmão.
 */
@SpringBootApplication(scanBasePackages = "com.barrier.webhookdelivery.testapp")
@EnableScheduling
public class TestApplication {}
