package com.example.db

import cats.effect.IO
import com.example.integration.PgIntegrationTest
import doobie.implicits.toConnectionIOOps

class UserRepositoryTest extends PgIntegrationTest {

  import UserRepository.*

  "it should read file" in withDatabase { xa =>
    for {
      id <- insert("test@test.com").transact(xa)
      userOpt <- findById(id).transact(xa)
    } yield {
      val _ = userOpt.isDefined shouldBe true
      userOpt.get.email shouldBe "test@test.com"
    }
  }

}
