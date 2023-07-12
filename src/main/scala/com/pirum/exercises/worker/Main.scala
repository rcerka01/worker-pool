package com.pirum.exercises.worker

import cats.effect.{IO, IOApp}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._

object Main extends IOApp.Simple {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val tasks = List(
    Task.Failure(3, 3.seconds),
    Task.Success(4, 4.seconds),
    Task.Success(2, 2.seconds),
    Task.Failure(1, 1.seconds)
  )
  val timeout: FiniteDuration = 3.seconds
  val workers = 4

  override def run: IO[Unit] = {
    for {
      executor <- TaskExecutor.make[IO]
      result <- executor.execute(tasks, timeout, workers)
      _ <- logger.info(s"execution result: $result")
    } yield ()
  }
}
