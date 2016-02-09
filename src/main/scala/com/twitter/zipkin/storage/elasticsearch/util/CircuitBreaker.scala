package com.twitter.zipkin.storage.elasticsearch.util

/**
  * Ported from https://github.com/akka/akka/blob/master/akka-actor/src/main/scala/akka/pattern/CircuitBreaker.scala
  * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
  */

import java.util.concurrent.atomic.{AtomicReference, AtomicInteger, AtomicLong, AtomicBoolean}
import com.twitter.conversions.time._
import com.twitter.util._

import scala.util.control.NoStackTrace
import java.util.concurrent.CopyOnWriteArrayList
import scala.util.control.NonFatal

/**
  * Companion object providing factory methods for Circuit Breaker which runs callbacks in caller's thread
  */
object CircuitBreaker {

  /**
    * Create a new CircuitBreaker.
    *
    * Callbacks run in caller's thread when using withSyncCircuitBreaker, and in same ExecutionContext as the passed
    * in Future when using withCircuitBreaker. To use another ExecutionContext for the callbacks you can specify the
    * executor in the constructor.
    *
    * @param maxFailures Maximum number of failures before opening the circuit
    * @param callTimeout [[com.twitter.util.Duration]] of time after which to consider a call a failure
    * @param resetTimeout [[com.twitter.util.Duration]] of time after which to attempt to close the circuit
    * @param filter Partial function that returns true if the exception should cause the circuit breaker to trip.
    */
  def apply(maxFailures: Int, callTimeout: Duration, resetTimeout: Duration, filter: PartialFunction[Throwable, Boolean], timer: Timer): CircuitBreaker =
    new CircuitBreaker(FuturePool.immediatePool, maxFailures, callTimeout, resetTimeout, filter, timer)

  def apply(ec: FuturePool, maxFailures: Int, callTimeout: Duration, resetTimeout: Duration, filter: PartialFunction[Throwable, Boolean], timer: Timer): CircuitBreaker =
    new CircuitBreaker(ec, maxFailures, callTimeout, resetTimeout, filter, timer)

}

/**
  * Provides circuit breaker functionality to provide stability when working with "dangerous" operations, e.g. calls to
  * remote systems
  *
  * Transitions through three states:
  * - In *Closed* state, calls pass through until the `maxFailures` count is reached.  This causes the circuit breaker
  * to open.  Both exceptions and calls exceeding `callTimeout` are considered failures.
  * - In *Open* state, calls fail-fast with an exception.  After `resetTimeout`, circuit breaker transitions to
  * half-open state.
  * - In *Half-Open* state, the first call will be allowed through, if it succeeds the circuit breaker will reset to
  * closed state.  If it fails, the circuit breaker will re-open to open state.  All calls beyond the first that
  * execute while the first is running will fail-fast with an exception.
  *
  * @param maxFailures Maximum number of failures before opening the circuit
  * @param callTimeout [[com.twitter.util.Duration]] of time after which to consider a call a failure
  * @param resetTimeout [[com.twitter.util.Duration]] of time after which to attempt to close the circuit
  * @param executor [[com.twitter.util.FuturePool]] used for execution of state transition listeners
  * @param filter Partial function that returns true if the exception should cause the circuit breaker to trip.
  */
class CircuitBreaker(executor: FuturePool,
                     maxFailures: Int,
                     callTimeout: Duration,
                     resetTimeout: Duration,
                     filter: PartialFunction[Throwable, Boolean], timer: Timer) {

  /**
    * Holds reference to current state of CircuitBreaker
    */
  private[this] val _currentStateDoNotCallMeDirectly = new AtomicReference[State](Closed)
  
  /**
    * Helper method for access to underlying state via Unsafe
    *
    * @param oldState Previous state on transition
    * @param newState Next state on transition
    * @return Whether the previous state matched correctly
    */
  @inline
  private[this] def swapState(oldState: State, newState: State): Boolean =
    _currentStateDoNotCallMeDirectly.compareAndSet(oldState, newState)

  /**
    * Helper method for accessing underlying state via Unsafe
    *
    * @return Reference to current state
    */
  @inline
  private[this] def currentState: State =
    _currentStateDoNotCallMeDirectly.get()

  /**
    * Wraps invocations of asynchronous calls that need to be protected
    *
    * @param body Call needing protected
    * @return [[scala.concurrent.Future]] containing the call result or a
    *   `scala.concurrent.TimeoutException` if the call timed out
    *
    */
  def withCircuitBreaker[T](body: => Future[T]): Future[T] = currentState.invoke(body)

  /**
    * Wraps invocations of synchronous calls that need to be protected
    *
    * Calls are run in caller's thread. Because of the synchronous nature of
    * this call the  `scala.concurrent.TimeoutException` will only be thrown
    * after the body has completed.
    *
    * Throws java.util.concurrent.TimeoutException if the call timed out.
    *
    * @param body Call needing protected
    * @return The result of the call
    */
  def withSyncCircuitBreaker[T](body: => T): T =
    Await.result(
      withCircuitBreaker(try Future.value(body) catch { case NonFatal(t) => Future.exception(t) }),
      callTimeout)

  /**
    * Adds a callback to execute when circuit breaker opens
    *
    * The callback is run in the [[com.twitter.util.FuturePool]] supplied in the constructor.
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onOpen(callback: => Unit): CircuitBreaker = {
    Open addListener { () => callback }
    this
  }

  /**
    * Adds a callback to execute when circuit breaker transitions to half-open
    *
    * The callback is run in the [[com.twitter.util.FuturePool]] supplied in the constructor.
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onHalfOpen(callback: => Unit): CircuitBreaker = {
    HalfOpen addListener { () => callback }
    this
  }

  /**
    * Adds a callback to execute when circuit breaker state closes
    *
    * The callback is run in the [[com.twitter.util.FuturePool]] supplied in the constructor.
    *
    * @param callback Handler to be invoked on state change
    * @return CircuitBreaker for fluent usage
    */
  def onClose(callback: => Unit): CircuitBreaker = {
    Closed addListener { () => callback }
    this
  }

  /**
    * Retrieves current failure count.
    *
    * @return count
    */
  private[util] def currentFailureCount: Int = Closed.get

  /**
    * Implements consistent transition between states. Throws IllegalStateException if an invalid transition is attempted.
    *
    * @param fromState State being transitioning from
    * @param toState State being transitioning from
    */
  private def transition(fromState: State, toState: State): Unit = {
    if (swapState(fromState, toState))
      toState.enter()
    // else some other thread already swapped state
  }

  /**
    * Trips breaker to an open state.  This is valid from Closed or Half-Open states.
    *
    * @param fromState State we're coming from (Closed or Half-Open)
    */
  private def tripBreaker(fromState: State): Unit = transition(fromState, Open)

  /**
    * Resets breaker to a closed state.  This is valid from an Half-Open state only.
    *
    */
  private def resetBreaker(): Unit = transition(HalfOpen, Closed)

  /**
    * Attempts to reset breaker by transitioning to a half-open state.  This is valid from an Open state only.
    *
    */
  private def attemptReset(): Unit = transition(Open, HalfOpen)

  /**
    * Internal state abstraction
    */
  private sealed trait State {
    private val listeners = new CopyOnWriteArrayList[() => Unit]

    /**
      * Add a listener function which is invoked on state entry
      *
      * @param listener listener implementation
      */
    def addListener(listener: () => Unit): Unit = listeners add listener

    /**
      * Test for whether listeners exist
      *
      * @return whether listeners exist
      */
    private def hasListeners: Boolean = !listeners.isEmpty

    /**
      * Notifies the listeners of the transition event via a Future executed in implicit parameter ExecutionContext
      *
      * @return Promise which executes listener in supplied [[scala.concurrent.ExecutionContext]]
      */
    protected def notifyTransitionListeners() {
      if (hasListeners) {
        val iterator = listeners.iterator
        while (iterator.hasNext) {
          val listener = iterator.next
          executor.apply(listener())
        }
      }
    }

    /**
      * Shared implementation of call across all states.  Thrown exception or execution of the call beyond the allowed
      * call timeout is counted as a failed call, otherwise a successful call
      *
      * @param body Implementation of the call
      * @return Future containing the result of the call
      */
    def callThrough[T](body: => Future[T]): Future[T] =
      Future(body).flatten.within(callTimeout)(timer) respond {
          case Return(x) =>
            callSucceeds()
          case Throw(t)  =>
            if (filter(t)) callFails() else callSucceeds()
        }

    /**
      * Abstract entry point for all states
      *
      * @param body Implementation of the call that needs protected
      * @return Future containing result of protected call
      */
    def invoke[T](body: => Future[T]): Future[T]

    /**
      * Invoked when call succeeds
      *
      */
    def callSucceeds(): Unit

    /**
      * Invoked when call fails
      *
      */
    def callFails(): Unit

    /**
      * Invoked on the transitioned-to state during transition.  Notifies listeners after invoking subclass template
      * method _enter
      *
      */
    final def enter(): Unit = {
      _enter()
      notifyTransitionListeners()
    }

    /**
      * Template method for concrete traits
      *
      */
    def _enter(): Unit
  }

  /**
    * Concrete implementation of Closed state
    */
  private object Closed extends AtomicInteger with State {

    /**
      * Implementation of invoke, which simply attempts the call
      *
      * @param body Implementation of the call that needs protected
      * @return Future containing result of protected call
      */
    override def invoke[T](body: => Future[T]): Future[T] = callThrough(body)

    /**
      * On successful call, the failure count is reset to 0
      *
      * @return
      */
    override def callSucceeds(): Unit = set(0)

    /**
      * On failed call, the failure count is incremented.  The count is checked against the configured maxFailures, and
      * the breaker is tripped if we have reached maxFailures.
      *
      * @return
      */
    override def callFails(): Unit = if (incrementAndGet() == maxFailures) tripBreaker(Closed)

    /**
      * On entry of this state, failure count is reset.
      *
      * @return
      */
    override def _enter(): Unit = set(0)

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Closed with failure count = " + get()
  }

  /**
    * Concrete implementation of half-open state
    */
  private object HalfOpen extends AtomicBoolean(true) with State {

    /**
      * Allows a single call through, during which all other callers fail-fast.  If the call fails, the breaker reopens.
      * If the call succeeds the breaker closes.
      *
      * @param body Implementation of the call that needs protected
      * @return Future containing result of protected call
      */
    override def invoke[T](body: => Future[T]): Future[T] =
      if (compareAndSet(true, false)) callThrough(body) else Future.exception[T](new CircuitBreakerOpenException(0.seconds))

    /**
      * Reset breaker on successful call.
      *
      * @return
      */
    override def callSucceeds(): Unit = resetBreaker()

    /**
      * Reopen breaker on failed call.
      *
      * @return
      */
    override def callFails(): Unit = tripBreaker(HalfOpen)

    /**
      * On entry, guard should be reset for that first call to get in
      *
      * @return
      */
    override def _enter(): Unit = set(true)

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Half-Open currently testing call for success = " + get()
  }

  /**
    * Concrete implementation of Open state
    */
  private object Open extends AtomicLong with State {

    /**
      * Fail-fast on any invocation
      *
      * @param body Implementation of the call that needs protected
      * @return Future containing result of protected call
      */
    override def invoke[T](body: => Future[T]): Future[T] =
      Future.exception[T](new CircuitBreakerOpenException(remainingDuration()))

    /**
      * Calculate remaining duration until reset to inform the caller in case a backoff algorithm is useful
      *
      * @return duration to when the breaker will attempt a reset by transitioning to half-open
      */
    private def remainingDuration(): Duration = {
      val diff = System.nanoTime() - get
      if (diff <= 0L) Duration.Zero
      else diff.nanoseconds
    }

    /**
      * No-op for open, calls are never executed so cannot succeed or fail
      *
      * @return
      */
    override def callSucceeds(): Unit = ()

    /**
      * No-op for open, calls are never executed so cannot succeed or fail
      *
      * @return
      */
    override def callFails(): Unit = ()

    /**
      * On entering this state, schedule an attempted reset via [[java.util.concurrent.ScheduledExecutorService]] and store the entry time to
      * calculate remaining time before attempted reset.
      *
      * @return
      */
    override def _enter(): Unit = {
      set(System.nanoTime())
      Future.Unit.delayed(resetTimeout)(timer).ensure(attemptReset())
      ()
    }

    /**
      * Override for more descriptive toString
      *
      * @return
      */
    override def toString: String = "Open"
  }

}

/**
  * Exception thrown when Circuit Breaker is open.
  *
  * @param remainingDuration Stores remaining time before attempting a reset.  Zero duration means the breaker is
  *                          currently in half-open state.
  * @param message Defaults to "Circuit Breaker is open; calls are failing fast"
  */
class CircuitBreakerOpenException(
                                   val remainingDuration: Duration,
                                   message: String = "Circuit Breaker is open; calls are failing fast")
  extends RuntimeException(message) with NoStackTrace