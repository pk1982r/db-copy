package com.example.model

import java.time.Instant

final case class User(
                       id: Long,
                       email: String,
                       createdAt: Instant
                     )
