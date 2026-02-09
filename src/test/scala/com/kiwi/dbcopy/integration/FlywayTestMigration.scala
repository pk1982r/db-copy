package com.kiwi.dbcopy.integration

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.flywaydb.core.Flyway

object FlywayTestMigration {

  def migrate(pgContainer: PostgreSQLContainer): Unit = {
    val flyway =
      Flyway
        .configure()
        .dataSource(
          pgContainer.jdbcUrl,
          pgContainer.username,
          pgContainer.password
        )
        .cleanDisabled(false) // enable clean ONLY in tests
        .load()

    flyway.clean()
    val _ = flyway.migrate()
  }
}