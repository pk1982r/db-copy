package com.kiwi.dbcopy

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.dimafeng.testcontainers.{DockerComposeContainer, PostgreSQLContainer}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

// First of all, you need to declare, which containers you want to use
class ContainerExampleSpec extends AnyFlatSpec with TestContainersForAll {

  // First of all, you need to declare, which containers you want to use
  override type Containers = PostgreSQLContainer and DockerComposeContainer

  override def startContainers(): Containers = {
    val container2 = PostgreSQLContainer.Def().start()
    val container3 =
      DockerComposeContainer
        .Def(DockerComposeContainer.ComposeFile(Left(new File("docker-compose.yml"))))
        .start()
    container2 and container3
  }

  it should "test" in withContainers { case pgContainer and dcContainer =>
    assert(pgContainer.jdbcUrl.nonEmpty && pgContainer.jdbcUrl.nonEmpty)
  }

}