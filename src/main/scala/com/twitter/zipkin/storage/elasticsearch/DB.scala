package com.twitter.zipkin.storage.elasticsearch

import java.util.concurrent.atomic.AtomicReference

import com.sksamuel.elastic4s.ElasticClient
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.zipkin.storage.elasticsearch.util.{CircuitBreakerConfig, CircuitBreaker}

import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import util.Config

import com.twitter.util.{Throw, Return, Future}

case class DB(config: Config, cbConfig: CircuitBreakerConfig, stats: StatsReceiver) {
  private def newClient =
    ElasticClient.remote(config.settings, config.host, config.port)

  private val client = new AtomicReference(newClient)

  private def exec(request: SearchRequest): Future[SearchResponse] =
    gatherMetrics {
      cb.withCircuitBreaker {
        val fal = FutureActionListener[SearchResponse]()
        client.get().client.search(request, fal)
        fal
      }
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
    }.onOpen {
      openCount.incr()
    }.onClose {
      closedCount.incr()
    }

  private def gatherMetrics[A](f: Future[A]): Future[A] =
    f respond {
      case Return(a) =>
        callCount.incr()
        successCount.incr()
      case Throw(t) =>
        callCount.incr()
        failureCount.incr()
    }

  def close(): Unit = {
    client.get().close()
    config.shield.foreach { _.ssl.foreach { _.trustStore.keystore.close() } }
  }
}
