package com.pirum.exercises.worker

import scala.concurrent.duration.FiniteDuration

// A task that either succeeds after n seconds, fails after n seconds, or never terminates
sealed trait Task {
  def id: Long
}

object Task {
  final case class Success(id: Long, delay: FiniteDuration) extends Task
  final case class Failure(id: Long, delay: FiniteDuration) extends Task
  final case class Timeout(id: Long) extends Task
}