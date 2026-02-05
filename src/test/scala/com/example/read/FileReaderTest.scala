package com.example.read

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.should

class FileReaderTest extends AsyncFreeSpec with AsyncIOSpec {

  "it should read file" in {
    val f = new Fixture
    f.testTxt.asserting(txt => txt should contain allOf(
      "7",
      "6 3",
      "3 8 5",
      "11 2 10 9",
    ))
  }

  class Fixture {
    val testTxt: IO[List[String]] = FileReader.readFromResource("data_tiny.txt").use(IO(_))
  }
}
