package com.example.copy

import com.example.db.UserRepository
import com.example.integration.TwoPgIntegrationTest
import com.example.model.User
import doobie.implicits.toConnectionIOOps

import java.time.Instant

class PostgresCopyAppTest extends TwoPgIntegrationTest {

  import UserRepository.*

  "it should have two separate DBs" in withDatabase { (xa, xb) =>
    val emailAUser = User("11L", "testA@test.com", Instant.now())
    val emailBUser = User("22L", "testB@test.com", Instant.now())
    for {
      _ <- insert(emailAUser).transact(xa)
      _ <- insert(emailBUser).transact(xb)

      userAinA <- findByEmail(emailAUser.email).transact(xa)
      userBinA <- findByEmail(emailBUser.email).transact(xa)

      userAinB <- findByEmail(emailAUser.email).transact(xb)
      userBinB <- findByEmail(emailBUser.email).transact(xb)
    } yield {
      val _ = userAinA.isDefined shouldBe true
      val _ = userAinA.get.email shouldBe emailAUser.email

      val _ = userBinA.isDefined shouldBe false

      val _ = userAinB.isDefined shouldBe false

      val _ = userBinB.isDefined shouldBe true
      userBinB.get.email shouldBe emailBUser.email
    }
  }
}
