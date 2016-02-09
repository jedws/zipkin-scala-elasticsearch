package com.twitter.zipkin.elasticsearch

import com.twitter.util.Timer
import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.elasticsearch.patterns.IndexPattern
import com.twitter.zipkin.storage.elasticsearch.{DB, ElasticSpanStore}
import com.twitter.zipkin.storage.SpanStore

case class SpanStoreBuilder(db: DB, patterns: List[IndexPattern], timer: Timer) extends Builder[SpanStore] { self =>
  def apply() = new ElasticSpanStore(db, patterns, timer)
}
