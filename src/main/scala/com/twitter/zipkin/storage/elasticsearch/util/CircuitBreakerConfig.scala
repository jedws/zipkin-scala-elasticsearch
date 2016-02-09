package com.twitter.zipkin.storage.elasticsearch.util

import com.twitter.util.{Timer, Duration, FuturePool}

case class CircuitBreakerConfig(executor: FuturePool, timer: Timer, maxFailures: Int, callTimeout: Duration, resetTimeout: Duration)
