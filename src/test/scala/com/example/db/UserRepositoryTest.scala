package com.example.db

import cats.effect.IO
import com.example.TestUserRepository
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

  "it should create many users" in withDatabase { xa =>
    val users = List.tabulate(1000)(i => i.userFromId)
    val users2 = List.tabulate(1000)(i => (1000 + i).userFromId)

    for {
      _ <- TestUserRepository.truncate.transact(xa)
      _ <- insertBatch_(users).transact(xa)
      _ <- insertBatch_(users2).transact(xa)
      numberOfUsers <- TestUserRepository.count.transact(xa)
    } yield numberOfUsers shouldBe 2000

  }

  extension (i: Int) {
    def userFromId: User = {
      User(s"$i", s"test$i@dot.com", Instant.now())
    }
  }
}
