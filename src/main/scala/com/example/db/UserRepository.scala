package com.example.db

import com.example.model.User
import doobie.*
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant

object UserRepository {

  def insert(email: String): ConnectionIO[Long] =
    sql"""
      INSERT INTO users (email)
      VALUES ($email)
      RETURNING id
    """.query[Long].unique

  def findById(id: Long): ConnectionIO[Option[User]] =
    sql"""
      SELECT id, email, created_at
      FROM users
      WHERE id = $id
    """.query[User].option

  def findAll: ConnectionIO[List[User]] =
    sql"""
      SELECT id, email, created_at
      FROM users
      ORDER BY id
    """.query[User].to[List]
}