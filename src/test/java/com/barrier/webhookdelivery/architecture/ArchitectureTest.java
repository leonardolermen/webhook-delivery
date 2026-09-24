package com.barrier.webhookdelivery.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** As regras da spec §5. Cada uma com nome, para a falha dizer qual fronteira caiu. */
@AnalyzeClasses(packages = "com.barrier.webhookdelivery", importOptions = ImportOption.DoNotIncludeTests.class)
public class ArchitectureTest {

  /** Guarda contra passar vacuamente sobre zero classes (ASM que não lê o bytecode do JDK). */
  @ArchTest
  static void o_import_enxerga_as_classes(JavaClasses classes) {
    assertThat(classes.size()).isGreaterThan(20);
  }

  @ArchTest
  static final ArchRule dominio_nao_conhece_spring_nem_jpa =
      noClasses().that().resideInAPackage("..domain..")
          .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "jakarta.persistence..", "..repository..", "..service..", "..client..", "..config..");

  @ArchTest
  static final ArchRule client_nao_conhece_repository =
      noClasses().that().resideInAPackage("..client..").should().dependOnClassesThat().resideInAPackage("..repository..");

  @ArchTest
  static final ArchRule ninguem_importa_commons_nem_kafka =
      noClasses().should().dependOnClassesThat().resideInAnyPackage("com.barrier.commons..", "org.apache.kafka..", "org.springframework.kafka..");

  @ArchTest
  static final ArchRule entidades_jpa_sao_package_private =
      com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes().that().areAnnotatedWith(jakarta.persistence.Entity.class)
          .should().bePackagePrivate();
}
