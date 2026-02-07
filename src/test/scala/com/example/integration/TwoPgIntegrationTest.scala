package com.example.integration

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.example.db.Database
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

  def withDatabase(
                    test: (HikariTransactor[IO], HikariTransactor[IO]) => IO[Assertion]
                  ): IO[Assertion] =
    withContainers { case a and b =>
      (transactor(a), transactor(b))
        .tupled
        .use { case (xa, xb) => test(xa, xb) }
    }
}