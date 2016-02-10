package com.twitter.zipkin
package storage
package elasticsearch
package util

import java.util.concurrent.TimeUnit

import com.twitter.finagle.stats.{Counter, Gauge, Stat, StatsReceiver}
import com.twitter.util.{Duration, FuturePool, Timer}

class ElasticSpanStoreSpec extends SpanStoreSpec {

  def clear: Unit = ()

  def store: SpanStore =
    new ElasticSpanStore(
      db = new DB(
        Config(
          host = "localhost"
          , portOption = None
          , clientTransportSniff = false
          , clusterName = "nocluster"
          , shield = None: Option[ShieldConfig]
        )
        , CircuitBreakerConfig(
          executor = FuturePool.immediatePool
          , timer = Timer.Nil
          , maxFailures = 0
          , callTimeout = Duration(1, TimeUnit.SECONDS)
          , resetTimeout = Duration(1, TimeUnit.SECONDS)
        )
        , stats = NullStatsReceiver
      )
      , patterns = List()
      , timer = Timer.Nil
    )
}

object NullStatsReceiver extends StatsReceiver {
  override val repr: AnyRef = this.getClass.getSimpleName

  override def counter(name: String*): Counter =
    new Counter {
      override def incr(delta: Int): Unit = ()
    }

  override def addGauge(name: String*)(f: => Float): Gauge =
    new Gauge {
      override def remove(): Unit = ()
    }

  override def stat(name: String*): Stat =
    new Stat {
      override def add(value: Float): Unit = ()
    }
}
