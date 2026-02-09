package com.example.integration

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.example.db.Database
import doobie.hikari.HikariTransactor
import org.scalatest.Assertion
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

  def withTransactor(pgTest: HikariTransactor[IO] => IO[Assertion]): IO[Assertion] =
    withContainers { c =>
      getTransactor(c).use(pgTest)
    }
}