package com.example.db

import cats.effect.IO
import com.example.PgIntegrationTest
import doobie.implicits.toConnectionIOOps

class UserRepositoryTest extends PgIntegrationTest {

  import UserRepository.*

  "it should read file" in {
    val io =
      for {
        xa <- xar
        id <- insert("test@test.com").transact(xa).toResource
        user <- findById(id).transact(xa).toResource
      } yield user

    io.asserting { userOpt =>
      userOpt.isDefined shouldBe true
      userOpt.get.email shouldBe "test@test.com"
    }.use(_ => IO.unit)
  }

}
