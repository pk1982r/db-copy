package com.kiwi.dbcopy.db

import cats.implicits.*
import com.kiwi.dbcopy.model.User
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*

object UserRepository {

  def insert(user: User): ConnectionIO[Long] =
    sql"""
      INSERT INTO users (external_id, email, created_at)
      VALUES (${user.externalId}, ${user.email}, ${user.createdAt})
      RETURNING id
    """
      .query[Long]
      .unique

  private val insertUserUpdate: Update[User] =
    Update[User](
      """
        INSERT INTO users (external_id, email, created_at)
        VALUES (?, ?, ?)
      """
    )

  def insertBatch(users: List[User]): ConnectionIO[List[Long]] =
    insertUserUpdate
      .updateManyWithGeneratedKeys[Long]("id")(users)
      .compile
      .toList

  def insertBatch_(users: List[User]): ConnectionIO[Int] =
    insertUserUpdate.updateMany(users)

  def findById(id: Long): ConnectionIO[Option[User]] =
    sql"""
      SELECT external_id, email, created_at
      FROM users
      WHERE id = $id
    """
      .query[User]
      .option

  def findByExternalId(externalId: String): ConnectionIO[Option[User]] =
    sql"""
      SELECT external_id, email, created_at
      FROM users
      WHERE external_id = $externalId
    """
      .query[User]
      .option

  def findByEmail(email: String): ConnectionIO[Option[User]] =
    sql"""
      SELECT external_id, email, created_at
      FROM users
      WHERE email = $email
    """
      .query[User]
      .option

  def findAll: ConnectionIO[List[User]] =
    sql"""
      SELECT external_id, email, created_at
      FROM users
      ORDER BY id
    """
      .query[User]
      .to[List]
}