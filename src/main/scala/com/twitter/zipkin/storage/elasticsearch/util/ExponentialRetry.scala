package com.twitter.zipkin.storage.elasticsearch.util

import com.twitter.conversions.time._
import com.twitter.util.{Future, Timer, Duration}

case class ExponentialRetry(
  base: Duration,
  factor: Double,
  maximum: Duration,
  timer: Timer
) {
  require(base > 0.seconds)
  require(factor >= 1)

  def apply[T](op: => Future[T]): Future[T] = {
    def retry(delay: Duration): Future[T] = {
      op rescue { case _ =>
        if (delay > maximum) {
          Future.exception(ExponentialRetry.Failure)
        } else {
          timer.doLater(delay) {
            retry((delay.inNanoseconds * factor).toLong.nanoseconds min maximum)
          }
        }
        .flatten
      }
    }
    retry(base)
  }
}

object ExponentialRetry {
  case object Failure extends Exception("Reached maximum number of retries")
}
