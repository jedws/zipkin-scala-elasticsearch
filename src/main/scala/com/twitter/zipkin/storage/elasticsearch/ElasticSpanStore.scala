package com.twitter.zipkin.storage.elasticsearch

import com.twitter.cache.Refresh

import scala.language.postfixOps
import java.util.{Date, Calendar}

import com.twitter.conversions.time._
import com.twitter.util._
import com.twitter.zipkin.common._
import com.twitter.zipkin.storage.elasticsearch.patterns.{ServiceName, IndexPattern}
import com.twitter.zipkin.storage.{QueryRequest, SpanStore}
import org.slf4j.LoggerFactory
import com.twitter.zipkin.adjuster.{ApplyTimestampAndDuration, CorrectForClockSkew, MergeById}

class ElasticSpanStore(db: DB, patterns: List[IndexPattern], timer: Timer) extends SpanStore {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val serviceNames = Refresh.every(10 minutes) {
    util.ExponentialRetry(base = 5 seconds, factor = 2.0, maximum = 5 minutes, timer = timer).apply {
      retrieveServiceNames
    }
  }

  private def retrieveServiceNames: Future[Seq[String]] =
    collect { _.searchServiceNames } map {
      _.map(_.self).sorted
    }

  override def apply(spans: Seq[Span]) = Future(())

  override def close() = {
    db.close()
  }

  private def index(pattern: IndexPattern): Index =
    Index(db, pattern, Calendar.getInstance.getTime)

  private def collect[A](f: Index => Search[Seq[A]]): Future[Seq[A]] =
    Future.collect(patterns.toSeq.map { p =>
      db.exec(f(index(p))) handle { case t =>
        log.warn(s"Failed to search index ${p(new Date())}", t)
        Seq.empty
      }
    }).map(_.flatten)

  private def groupAndSortSpans(spans: Seq[Span]): Seq[List[Span]] =
    spans.groupBy(_.traceId).values
      .filter(_.nonEmpty)
      .map(MergeById) // also sorts
      .map(CorrectForClockSkew)
      .map(ApplyTimestampAndDuration)
      .filter(_.nonEmpty) // somehow the list of span may have become empty
      .toSeq
      .sortBy(_.head)(Ordering[Span].reverse) // sort descending by the first span

  private def toIndexSpanSearchRequest(qr: QueryRequest): IndexSpanSearchRequest =
    IndexSpanSearchRequest(serviceName = ServiceName(qr.serviceName),
                           spanName = qr.spanName,
                           annotations = qr.annotations.map({a => (a, None)}) ++
                                          qr.binaryAnnotations.map({case (a, b) => (a, Some(b))}),
                           endTsMs = qr.endTs,
                           lookbackMs = qr.lookback,
                           limit = qr.limit)

  override def getTraces(qr: QueryRequest): Future[Seq[List[Span]]] =
    collect { _.searchTraces(toIndexSpanSearchRequest(qr)) } map groupAndSortSpans

  override def getTracesByIds(traceIds: Seq[Long]): Future[Seq[List[Span]]] =
    collect { _.searchTracesByIds(traceIds) } map groupAndSortSpans

  override def getAllServiceNames: Future[Seq[String]] =
    serviceNames()

  override def getSpanNames(service: String): Future[Seq[String]] =
    collect { _.searchSpanNames(ServiceName(service)) map { _.sorted } }
}
