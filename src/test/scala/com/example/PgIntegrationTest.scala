package com.example

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.example.db.Database
import doobie.hikari.HikariTransactor
import org.flywaydb.core.Flyway
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

trait PgIntegrationTest extends AsyncFreeSpec with AsyncIOSpec with TestContainersForAll with Matchers {
  override type Containers = PostgreSQLContainer

  override def startContainers(): PostgreSQLContainer = {
    val container = PostgreSQLContainer.Def().start()

    FlywayTestMigration.migrate(container)

    container
  }

  private def getTransactor(container: PostgreSQLContainer): Resource[IO, HikariTransactor[IO]] = Database.transactor(container.jdbcUrl, container.username, container.password)

  val container: PostgreSQLContainer = startContainers()
  val xar: Resource[IO, HikariTransactor[IO]] = getTransactor(container)
}


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
    flyway.migrate()
  }
}