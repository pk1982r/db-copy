package com.example.read

import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import org.scalatest.flatspec.AnyFlatSpec
import java.io.File

// First of all, you need to declare, which containers you want to use
class ContainerExampleSpec extends AnyFlatSpec with TestContainersForAll {

  // First of all, you need to declare, which containers you want to use
  override type Containers = PostgreSQLContainer and DockerComposeContainer

  // After that, you need to describe, how you want to start them,
  // In this method you can use any intermediate logic.
  // You can pass parameters between containers, for example.
  override def startContainers(): Containers = {
    val container2 = PostgreSQLContainer.Def().start()
    val container3 =
      DockerComposeContainer
        .Def(DockerComposeContainer.ComposeFile(Left(new File("docker-compose.yml"))))
        .start()
    container2 and container3
  }

  // `withContainers` function supports multiple containers:
  it should "test" in withContainers { case pgContainer and dcContainer =>
    // Inside your test body you can do with your containers whatever you want to
    assert(pgContainer.jdbcUrl.nonEmpty && pgContainer.jdbcUrl.nonEmpty)
  }

}