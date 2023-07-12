package com.pirum.exercises.worker

import cats.effect.Temporal
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Stream
import org.typelevel.log4cats.Logger

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

/**
 * It's a sealed trait representing the possible outcomes of a task execution.
 * It has three concrete case classes: Successful, Failed, and TimedOut, each containing a taskId representing the ID of the task.
 */
sealed trait ExecutionOutcome {
  def taskId: Long
}

object ExecutionOutcome {
  final case class Successful(taskId: Long) extends ExecutionOutcome
  final case class Failed(taskId: Long) extends ExecutionOutcome
  final case class TimedOut(taskId: Long) extends ExecutionOutcome
}

/**
 * It's a case class representing the result of task execution.
 * It contains three lists: successful, failed, and timedOut, which store the IDs of tasks based on their execution outcome.
 */
final case class ExecutionResult(
    successful: List[Long],
    failed: List[Long],
    timedOut: List[Long]
)

object ExecutionResult {
  def from(outcomes: List[ExecutionOutcome]): ExecutionResult =
    outcomes.foldLeft(ExecutionResult(Nil, Nil, Nil)) { (result, outcome) =>
      outcome match {
        case ExecutionOutcome.Successful(taskId) =>
          result.copy(successful = result.successful :+ taskId)
        case ExecutionOutcome.Failed(taskId) =>
          result.copy(failed = result.failed :+ taskId)
        case ExecutionOutcome.TimedOut(taskId) =>
          result.copy(timedOut = result.timedOut :+ taskId)
      }
    }
}

/**
 *  implements the TaskExecutor trait. It takes implicit instances of Temporal[F] and Logger[F] as constructor arguments.
 *  The execute method takes a list of Task instances, a timeout duration, and the number of workers.
 *  It executes the tasks concurrently using fs2's Stream and returns an F[ExecutionResult] representing the final result.
 */
trait TaskExecutor[F[_]] {
  def execute(
      tasks: List[Task],
      timeout: FiniteDuration,
      workers: Int
  ): F[ExecutionResult]
}

/**
 *
 * @param F
 * It's a type class representing effect types that support timed operations, such as sleep and timeout.
 * @param logger
 * It's a type class representing effect types that support logging operations.
 */
final private class LiveTaskExecutor[F[_]](implicit
    F: Temporal[F],
    logger: Logger[F]
) extends TaskExecutor[F] {

  override def execute(
      tasks: List[Task],
      timeout: FiniteDuration,
      workers: Int
  ): F[ExecutionResult] = {
    //  It's a streaming library provided by the fs2 library.
    //  It allows working with streams of data and provides combinators to transform and manipulate streams.
    Stream
      // The tasks list is converted into a stream
      .emits(tasks)
      .map { task =>
        Stream
          // task performed
          .eval(run(task))
          // if Successful
          .map(_ => ExecutionOutcome.Successful(task.id))
          // timeout applied
          .timeout(timeout)
          // if Failed
          .handleErrorWith {
            case _: TimeoutException =>
              Stream(ExecutionOutcome.TimedOut(task.id))
            case _ =>
              Stream(ExecutionOutcome.Failed(task.id))
          }
      }
      // A timeout is applied to the task stream using .timeout(timeout).
      // If the task exceeds the specified timeout duration, the stream emits an ExecutionOutcome.TimedOut.
      .parJoin(workers)
      // The execution outcome for each task is logged using evalTap
      .evalTap {
        case ExecutionOutcome.Successful(id) =>
          logger.info(s"Task$id completed successfully")
        case ExecutionOutcome.Failed(id)   => logger.info(s"Task$id failed")
        case ExecutionOutcome.TimedOut(id) => logger.info(s"Task$id timed out")
      }
      // The stream is compiled into a list using, then converted to Execution Result
      .compile
      .toList
      .map(ExecutionResult.from)
  }

  /**
   * It is responsible for executing the task and returning an F[Unit] effect.
   * The resulting stream is transformed into an ExecutionOutcome.Successful if the task completes successfully.
   */
  private def run(task: Task): F[Unit] =
    task match {
      case Task.Success(id, delay) =>
        logger.info(s"Task$id completes after $delay") >>
          F.sleep(delay) >>
          F.unit
      case Task.Failure(id, delay) =>
        logger.info(s"Task$id fails after $delay") >>
          F.sleep(delay) >>
          F.raiseError[Unit](new RuntimeException("uh oh"))
      case Task.Timeout(id) =>
        logger.info(s"Task$id hangs") >>
          F.never[Unit]
    }
}

/**
 * The TaskExecutor companion object provides a factory method make to create instances of TaskExecutor,
 * using the required type class instances Temporal[F] and Logger[F].
 */
object TaskExecutor {
  def make[F[_]: Temporal: Logger]: F[TaskExecutor[F]] =
    Temporal[F].pure(new LiveTaskExecutor[F])
}
