package com.kiwi.dbcopy.integration

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.kiwi.dbcopy.copy.PostgresCopyApp.DbConfig
import com.kiwi.dbcopy.db.Database
import doobie.hikari.HikariTransactor
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

trait TwoPgIntegrationTest
  extends AsyncFreeSpec
    with AsyncIOSpec
    with TestContainersForAll
    with Matchers {

  override type Containers =
    PostgreSQLContainer and PostgreSQLContainer

  override def startContainers(): Containers = {
    val a = PostgreSQLContainer.Def().start()
    val b = PostgreSQLContainer.Def().start()

    FlywayTestMigration.migrate(a)
    FlywayTestMigration.migrate(b)

    a and b
  }

  private def transactor(
                          c: PostgreSQLContainer
                        ): Resource[IO, HikariTransactor[IO]] =
    Database.transactor(c.jdbcUrl, c.username, c.password)

  def withTransactors(
                       test: (HikariTransactor[IO], HikariTransactor[IO]) => IO[Assertion]
                     ): IO[Assertion] =
    withContainers { case a and b =>
      (transactor(a), transactor(b))
        .tupled
        .use { case (xa, xb) => test(xa, xb) }
    }

  case class IntegrationDb(config: DbConfig, transactor: HikariTransactor[IO])

  case class IntegrationDbConfig(dbA: IntegrationDb, dbB: IntegrationDb)

  def withDatabases(
                     test: IntegrationDbConfig => IO[Assertion]
                   ): IO[Assertion] =
    withContainers { case pgContainerA and pgContainerB =>
      (transactor(pgContainerA), transactor(pgContainerB))
        .tupled
        .use { case (xa, xb) => test(IntegrationDbConfig(
          IntegrationDb(configFromContainer(pgContainerA), xa),
          IntegrationDb(configFromContainer(pgContainerB), xb),
        ))
        }
    }

  private def configFromContainer(pgContainerA: PostgreSQLContainer): DbConfig =
    DbConfig(
      host = pgContainerA.host,
      port = pgContainerA.mappedPort(5432),
      database = pgContainerA.databaseName,
      user = pgContainerA.username,
      password = pgContainerA.password,
      schema = "public"
    )

}