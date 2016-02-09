package com.twitter.zipkin.storage.elasticsearch

import java.util.concurrent.atomic.AtomicReference

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.HashedWheelTimer
import com.twitter.zipkin.storage.elasticsearch.util.{CircuitBreakerConfig, CircuitBreaker}
import org.elasticsearch.action.ActionListener

import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
//import org.elasticsearch.common.settings.{ Settings => ImmutableSettings } Version 2.1
import util.Config

import com.twitter.util.{Throw, Return, Promise, Future}

import com.sksamuel.elastic4s._

sealed trait Search[R] {
  def map[S](f: R => S): Search[S] =
    fold(f andThen Result.apply, { (query, filter) => Query(query, filter andThen f) })

  def fold[X](result: R => X, query: (SearchDefinition, SearchResponse => R) => X): X =
    this match {
      case Result(r) => result(r)
      case Query(q, f) => query(q, f)
    }
}
case class Result[R](value: R) extends Search[R]
case class Query[R](query: SearchDefinition, filter: SearchResponse => R) extends Search[R]

object Search {
  def apply(query: SearchDefinition): Search[SearchResponse] =
    Query(query, identity)

  def ok[R](r: R): Search[R] =
    Result(r)
}

case class DB(config: Config, cbConfig: CircuitBreakerConfig, stats: StatsReceiver) {
  private def newClient =
    ElasticClient.remote(config.settings, config.host, config.port)

  private val client = new AtomicReference(newClient)

  private def exec(request: SearchRequest): Future[SearchResponse] = {
    gatherMetrics(cb.withCircuitBreaker {
      val fal = FutureActionListener[SearchResponse]()
      client.get().client.search(request, fal)
      fal
    })
  }

  def exec[R](s: Search[R]): Future[R] =
    s.fold(Future.value, { (query, filter) => exec(query.build).map(filter) })

  private val openCount = stats.counter("db.circuitbreaker.open")
  private val halfOpenCount = stats.counter("db.circuitbreaker.halfopen")
  private val closedCount = stats.counter("db.circuitbreaker.closed")

  private val callCount = stats.counter("db.circuitbreaker.call.times")
  private val successCount = stats.counter("db.circuitbreaker.call.success")
  private val failureCount = stats.counter("db.circuitbreaker.call.failure")

  private val cb =
    CircuitBreaker(cbConfig.executor, cbConfig.maxFailures, cbConfig.callTimeout, cbConfig.resetTimeout, {
      case e: Throwable =>
        e.getCause match {
          case x: org.elasticsearch.indices.IndexMissingException => false        // In case the index doesn't exist already
          case x: org.elasticsearch.shield.authz.AuthorizationException => false  // In case index permissions are not correctly configured yet
          case _ => true
        }
    }, cbConfig.timer).onHalfOpen {
      halfOpenCount.incr()
      client.getAndSet(newClient).close()
    }.onClose {
      closedCount.incr()
    }.onOpen {
      openCount.incr()
    }

  private def gatherMetrics[A](f: Future[A]): Future[A] = {
    f respond {
      case Return(a) =>
        callCount.incr()
        successCount.incr()
      case Throw(t) =>
        callCount.incr()
        failureCount.incr()
    }
  }

  def close() = {
    client.get().close()
    config.shield.foreach { _.ssl.foreach { _.trustStore.keystore.close() } }
  }
}

class FutureActionListener[A] extends Promise[A] with ActionListener[A] {
  def onFailure(e: Throwable) = setException(e)
  def onResponse(a: A) = setValue(a)
}
object FutureActionListener {
  def apply[A]() = new FutureActionListener[A]
}
