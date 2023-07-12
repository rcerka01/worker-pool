package com.pirum.exercises.worker

import cats.effect.IO
import scala.concurrent.duration.DurationInt
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AsyncFreeSpec
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class MainSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  "Workers must correctly finish" - {
    "single successful task" in {
      val tasks = List(Task.Success(1, 1.seconds))
      val timeout = 2.seconds
      val workers = 1
      val expected = ExecutionResult(List(1), List(), List())

      for {
        executor <- TaskExecutor.make[IO]
        result <- executor.execute(tasks, timeout, workers)
      } yield assert(result == expected)
    }

    "single failing task" in {
      val tasks = List(Task.Failure(1, 1.seconds))
      val timeout = 2.seconds
      val workers = 1
      val expected = ExecutionResult(List(), List(1), List())

      for {
        executor <- TaskExecutor.make[IO]
        result <- executor.execute(tasks, timeout, workers)
      } yield assert(result == expected)
    }

    "single timeout task" in {
      val tasks = List(Task.Success(1, 3.seconds))
      val timeout = 2.seconds
      val workers = 1
      val expected = ExecutionResult(List(), List(), List(1))

      for {
        executor <- TaskExecutor.make[IO]
        result <- executor.execute(tasks, timeout, workers)
      } yield assert(result == expected)
    }

    "composition with successful and failing tasks right after tasks are complete" in {
      val tasks = List(
        Task.Failure(3, 3.seconds),
        Task.Success(4, 4.seconds),
        Task.Success(2, 2.seconds),
        Task.Failure(1, 1.seconds)
      )
      val timeout = 8.seconds
      val workers = 4
      val expected = ExecutionResult(List(2, 4), List(1, 3), List())

      for {
        executor <- TaskExecutor.make[IO]
        t1 = System.currentTimeMillis
        result <- executor.execute(tasks, timeout, workers)
        executionTime = System.currentTimeMillis - t1
      } yield assert(
        result == expected &&
          executionTime > 4000 &&
          executionTime < 5000
      )
    }

    "composition with successful, failing and timed out tasks" in {
      val tasks = List(
        Task.Failure(3, 3.seconds),
        Task.Success(5, 100.seconds),
        Task.Success(4, 4.seconds),
        Task.Success(2, 2.seconds),
        Task.Success(1, 1.seconds)
      )
      val timeout = 8.seconds
      val workers = 5
      val expected = ExecutionResult(List(1, 2, 4), List(3), List(5))

      for {
        executor <- TaskExecutor.make[IO]
        t1 = System.currentTimeMillis
        result <- executor.execute(tasks, timeout, workers)
        executionTime = System.currentTimeMillis - t1
      } yield assert(
        result == expected &&
          executionTime < 9000
      )
    }
  }
}
