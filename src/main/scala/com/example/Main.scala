package com.example

import cats.effect.{IO, IOApp}

object Main extends IOApp.Simple {

  val run: IO[Unit] = for {
    _ <- IO.println("Hello word")
  } yield ()
}
