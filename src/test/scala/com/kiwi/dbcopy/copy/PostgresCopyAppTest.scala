package com.kiwi.dbcopy.copy

import com.kiwi.dbcopy.copy.PostgresCopyApp.CopyConfig
import com.kiwi.dbcopy.db.UserRepository
import com.kiwi.dbcopy.integration.TwoPgIntegrationTest
import com.kiwi.dbcopy.model.User
import com.kiwi.dbcopy.{TestUserRepository, userFromId}
import doobie.implicits.toConnectionIOOps

import java.time.Instant
import scala.concurrent.duration.DurationInt

class PostgresCopyAppTest extends TwoPgIntegrationTest {

  import UserRepository.*

  "it should have two separate DBs" in withTransactors { (xa, xb) =>
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

  private val copyConfig = CopyConfig("SELECT * FROM users", "users", None, 1000, 1000.millis)

  "it should copy using the script - nothing to copy" in withDatabases { dbs =>
    for {
      _ <- TestUserRepository.truncate.transact(dbs.dbA.transactor)
      _ <- TestUserRepository.truncate.transact(dbs.dbB.transactor)
      _ = PostgresCopyApp.performCopy(copyConfig, dbs.dbA.config, dbs.dbB.config)
      numberOfUsersA <- TestUserRepository.count.transact(dbs.dbA.transactor)
      numberOfUsersB <- TestUserRepository.count.transact(dbs.dbB.transactor)
    } yield {
      val _ = numberOfUsersA shouldBe 0
      numberOfUsersB shouldBe 0
    }
  }

  "is should copy using the script" in withDatabases { dbs =>
    val numberOfUsers = 1000
    for {
      _ <- TestUserRepository.truncate.transact(dbs.dbA.transactor)
      _ <- TestUserRepository.truncate.transact(dbs.dbB.transactor)
      users = List.tabulate(numberOfUsers)(_.userFromId)
      _ <- insertBatch_(users).transact(dbs.dbA.transactor)
      copied <- PostgresCopyApp.performCopy(copyConfig, dbs.dbA.config, dbs.dbB.config)
      numberOfUsersA <- TestUserRepository.count.transact(dbs.dbA.transactor)
      numberOfUsersB <- TestUserRepository.count.transact(dbs.dbB.transactor)
    } yield {
      val _ = copied shouldBe numberOfUsers
      val _ = numberOfUsersA shouldBe numberOfUsers
      numberOfUsersB shouldBe numberOfUsers
    }
  }
}
