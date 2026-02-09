package com.example.db

import cats.effect.{IO, Resource}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts

object Database {

  def transactor(
                  jdbcUrl: String,
                  user: String,
                  password: String
                ): Resource[IO, HikariTransactor[IO]] =
    for
      te <- ExecutionContexts.cachedThreadPool[IO]
      xa <- HikariTransactor.newHikariTransactor[IO](
        "org.postgresql.Driver",
        jdbcUrl,
        user,
        password,
        te,
      )
    yield xa
}