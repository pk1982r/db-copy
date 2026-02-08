package com.example.db

import cats.effect.IO
import com.example.integration.PgIntegrationTest
import com.example.model.User
import doobie.implicits.toConnectionIOOps

import java.time.Instant

class UserRepositoryTest extends PgIntegrationTest {

  import UserRepository.*

  "it should read file" in withDatabase { xa =>
    for {
      id <- insert(User("1L", "test@test.com", Instant.now())).transact(xa)
      userOpt <- findById(id).transact(xa)
    } yield {
      val _ = userOpt.isDefined shouldBe true
      userOpt.get.email shouldBe "test@test.com"
    }
  }

}
