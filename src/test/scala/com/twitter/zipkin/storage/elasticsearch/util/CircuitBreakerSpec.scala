package com.twitter.zipkin.storage.elasticsearch.util

import java.util.concurrent.{ TimeUnit, CountDownLatch }

import com.twitter.util._

import scala.concurrent.TimeoutException
import com.twitter.conversions.time._

import scala.reflect.ClassTag

class CircuitBreakerSpec extends org.specs2.mutable.SpecificationWithJUnit {

  def run[A](f: Future[A]): A = Await.result(f)

  private def fail[A](t: Throwable) = Future.exception(t)

  private def delay[A](a: => A) = pool(a)

  "An asynchronous circuit breaker that is open" should {
    "throw exceptions when called before reset timeout" in {
      val breaker = longResetTimeoutCb()
      breaker().withCircuitBreaker(fail(new TestException))
      checkLatch(breaker.openLatch) and
        intercept[CircuitBreakerOpenException] { run(breaker().withCircuitBreaker(delay(sayHi))) }
    }

    "transition to half-open on reset timeout" in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(fail(new TestException))
      checkLatch(breaker.halfOpenLatch)
    }
  }

  "An asynchronous circuit breaker that is half-open" should {
    "pass through next call and close on success" in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(fail(new TestException))
      checkLatch(breaker.halfOpenLatch) and
        (run(breaker().withCircuitBreaker(delay(sayHi))) must_===("hi")) and
        checkLatch(breaker.closedLatch)
    }

    "re-open on exception in call" in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(fail(new TestException))
      checkLatch(breaker.halfOpenLatch) and
        intercept[TestException]({ run(breaker().withCircuitBreaker(fail(new TestException))) }) and
        checkLatch(breaker.openLatch)
    }

    "re-open on async failure" in {
      val breaker = shortResetTimeoutCb()
      breaker().withCircuitBreaker(fail(new TestException))
      checkLatch(breaker.halfOpenLatch) and {
        breaker().withCircuitBreaker(fail(new TestException))
        checkLatch(breaker.openLatch)
      }
    }
  }

  "An asynchronous circuit breaker that is closed" should {
    "allow calls through" in {
      val breaker = longCallTimeoutCb()
      run(breaker().withCircuitBreaker(delay(sayHi))) must_===("hi")
    }

    "increment failure count on exception" in {
      val breaker = longCallTimeoutCb()
      intercept[TestException]({ run(breaker().withCircuitBreaker(fail(new TestException))) }) and
        checkLatch(breaker.openLatch) and
        (breaker().currentFailureCount must_===(1))
    }

    "increment failure count on async failure" in {
      val breaker = longCallTimeoutCb()
      breaker().withCircuitBreaker(fail(new TestException))
      checkLatch(breaker.openLatch) and
        (breaker().currentFailureCount must_===(1))
    }

    "reset failure count after success" in {
      val breaker = multiFailureCb()
      breaker().withCircuitBreaker(delay(sayHi))
      for (n <- 1 to 4) breaker().withCircuitBreaker(fail(new TestException))
      awaitCond(breaker().currentFailureCount == 4) and {
        breaker().withCircuitBreaker(delay(sayHi))
        awaitCond(breaker().currentFailureCount == 0)
      }
    }

    "increment failure count on callTimeout" in {
      val breaker = shortCallTimeoutCb()

      val fut = breaker().withCircuitBreaker(delay {
        Thread.sleep(150)
        throwException
      })
      checkLatch(breaker.openLatch)
      (breaker().currentFailureCount must_===(1)) and
      // Since the timeout should have happend before the inner code finishes
      // we expect a timeout, not TestException
      intercept[TimeoutException] {
        run(fut)
      }

    }
  }

  private val failureHandler: PartialFunction[Throwable, Boolean] =
    { case _ => true }

  private val timer = new ScheduledThreadPoolTimer()

  private val pool = FuturePool.interruptibleUnboundedPool

  private def shortCallTimeoutCb(): Breaker =
    new Breaker(CircuitBreaker(pool, 1, 50.millis, 500.millis, failureHandler, timer))

  private def shortResetTimeoutCb(): Breaker =
    new Breaker(CircuitBreaker(pool, 1, 1000.millis, 50.millis, failureHandler, timer))

  private def longCallTimeoutCb(): Breaker =
    new Breaker(CircuitBreaker(pool, 1, 5.seconds, 500.millis, failureHandler, timer))

  private val longResetTimeout = 5.seconds
  private def longResetTimeoutCb(): Breaker =
    new Breaker(CircuitBreaker(pool, 1, 100.millis, longResetTimeout, failureHandler, timer))

  private def multiFailureCb(): Breaker =
    new Breaker(CircuitBreaker(pool, 5, 200.millis, 500.millis, failureHandler, timer))

  private def intercept[E <: Throwable](block: => Any)(implicit m: ClassTag[E]) = block must throwA[E]
  
  private def throwException = throw new TestException

  private def sayHi = "hi"

  protected final val awaitTimeout = 2.seconds

  private def checkLatch(latch: TestLatch) = {
    latch.check()
    ok
  }

  private def now: Duration = Duration.fromNanoseconds(System.nanoTime())

  private def awaitCond(p: => Boolean, max: Duration = 1.minute, interval: Duration = 100.millis, message: String = "") = {
    val stop = now + max

    @scala.annotation.tailrec
    def poll(t: Duration) {
      if (!p) {
        assert(now < stop, s"timeout $max expired: $message")
        Thread.sleep(t.inUnit(TimeUnit.MILLISECONDS))
        poll((stop - now) min interval)
      }
    }
    poll(max min interval)
    ok
  }

  private class TestException extends RuntimeException

  private class Breaker(val instance: CircuitBreaker) {
    val halfOpenLatch = new TestLatch(1)
    val openLatch = new TestLatch(1)
    val closedLatch = new TestLatch(1)
    def apply(): CircuitBreaker = instance
    instance.onClose(closedLatch.countDown()).onHalfOpen(halfOpenLatch.countDown()).onOpen(openLatch.countDown())
  }

  class TestLatch(count: Int = 1) {
    private var latch = new CountDownLatch(count)

    def countDown(): Unit = latch.countDown()
    def isOpen: Boolean = latch.getCount == 0
    def open(): Unit = while (!isOpen) countDown()
    def reset(): Unit = latch = new CountDownLatch(count)

    def check(): Unit = {
      val opened = latch.await(awaitTimeout.inNanoseconds, TimeUnit.NANOSECONDS)
      if (!opened) throw new TimeoutException(
        "Timeout of %s" format awaitTimeout.toString)
    }
  }
}
