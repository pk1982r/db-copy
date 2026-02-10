package com.kiwi.dbcopy

import cats.implicits.*
import doobie.*
import doobie.implicits.*

object TestUserRepository {

  /** Truncate users table (fast, resets state) */
  def truncate: ConnectionIO[Unit] =
    sql"TRUNCATE TABLE users RESTART IDENTITY CASCADE"
      .update
      .run
      .void

  def truncateBackup: ConnectionIO[Unit] =
    sql"TRUNCATE TABLE users_backup RESTART IDENTITY CASCADE"
      .update
      .run
      .void

  /** Count number of users */
  def count: ConnectionIO[Long] =
    sql"SELECT COUNT(*) FROM users"
      .query[Long]
      .unique

  def countBackup: ConnectionIO[Long] =
    sql"SELECT COUNT(*) FROM users_backup"
      .query[Long]
      .unique
}
