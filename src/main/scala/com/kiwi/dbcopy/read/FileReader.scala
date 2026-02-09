package com.kiwi.dbcopy.read

import cats.effect.{IO, Resource}
import fs2.io.readInputStream
import fs2.text
import fs2.text.{lines, utf8}

object FileReader {

  private def resourceStream(name: String): Resource[IO, java.io.InputStream] =
    Resource.fromAutoCloseable(IO {
      Option(getClass.getResourceAsStream(s"/$name"))
        .getOrElse(throw new RuntimeException(s"Missing resource: $name"))
    })

  def readFromResource(name: String): Resource[IO, List[String]] =
    readInputStream(resourceStream(name), chunkSize = 8192)
      .through(utf8.decode)
      .through(lines)
      //.drop(2)
      //.intersperse("\n")
      .compile
      .toList
}
