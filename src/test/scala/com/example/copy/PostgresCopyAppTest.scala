package com.example.copy

import com.example.db.UserRepository
import com.example.integration.TwoPgIntegrationTest
import doobie.implicits.toConnectionIOOps

class PostgresCopyAppTest extends TwoPgIntegrationTest {

  import UserRepository.*

  "it should have two separate DBs" in withDatabase { (xa, xb) =>
    val emailA = "testA@test.com"
    val emailB = "testB@test.com"
    for {
      _ <- insert(emailA).transact(xa)
      _ <- insert(emailB).transact(xb)

      userAinA <- findByEmail(emailA).transact(xa)
      userBinA <- findByEmail(emailB).transact(xa)

      userAinB <- findByEmail(emailA).transact(xb)
      userBinB <- findByEmail(emailB).transact(xb)
    } yield {
      val _ = userAinA.isDefined shouldBe true
      val _ = userAinA.get.email shouldBe emailA

      val _ = userBinA.isDefined shouldBe false

      val _ = userAinB.isDefined shouldBe false

      val _ = userBinB.isDefined shouldBe true
      userBinB.get.email shouldBe emailB
    }
  }
}
