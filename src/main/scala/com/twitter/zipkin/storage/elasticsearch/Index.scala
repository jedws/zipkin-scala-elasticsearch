package com.twitter.zipkin.storage.elasticsearch

import java.util.Date

import com.twitter.util.Try
import com.twitter.zipkin.storage.elasticsearch.patterns.{ServiceName, IndexPattern}
import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.collection.JavaConverters._

import com.twitter.finagle.tracing.SpanId
import com.twitter.zipkin.common._

import org.elasticsearch.search.aggregations.bucket.terms.StringTerms
import org.elasticsearch.search.sort.SortOrder

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl._

case class IndexSpanSearchRequest(serviceName: ServiceName,
                                  spanName: Option[String],
                                  annotations: Set[(String, Option[String])],
                                  endTsMs: Long,
                                  lookbackMs: Long,
                                  limit: Int)

object Index {
  private val log = LoggerFactory.getLogger(this.getClass)
}

case class Index(db: DB, pattern: IndexPattern, date: Date) {

  import Index._

  private val indexName = pattern(date)

  implicit object SpanHitAs extends HitAs[Option[Span]] {
    override def as(hit: RichSearchHit): Option[Span] =
      pattern.decodeSpan(hit.sourceAsString)
  }

  private def filterService[R](service: ServiceName)(f: String => Search[Seq[R]]): Search[Seq[R]] =
    pattern.fields.serviceName.toIndex(service).fold(Search.ok(Seq.empty[R])) { f }

  def searchTraces(req: IndexSpanSearchRequest): Search[Seq[Span]] =
    filterService(req.serviceName) { serviceName =>
      Search {
        search in indexName limit req.limit query {
          filteredQuery.filter {
            must {
              List(
                termFilter(pattern.fields.serviceName.name, serviceName),
                req.spanName.map {
                  termFilter(pattern.fields.spanName.name, _)
                } getOrElse existsFilter(pattern.fields.traceId.name)
              ) ++
                req.annotations.map { case (key, value) =>
                  pattern.translateBinaryAnnotation(key).map { fieldName =>
                    value match {
                      case None => existsFilter(fieldName)
                      case Some(v) => queryFilter(wildcardQuery(fieldName, v))
                    }
                  }.toList
                }.toList.flatten
            }
            // TODO - add endTs and lookback
          }
        } sort (fieldSort("@timestamp") order SortOrder.DESC)
      } map { r =>
        Try {
          r.as[Option[Span]].flatten.toSeq
        }.onFailure {
          e => log.error(s"No traces returned for service $serviceName in index $indexName. Service name field ${pattern.fields.serviceName.name}. Span name field ${pattern.fields.spanName.name}. Trace Id field ${pattern.fields.traceId.name}. Hit count ${r.getHits.totalHits}", e)
        }.getOrElse(Seq.empty)
      }
    }

  def searchSpanNames(serviceName: ServiceName): Search[Seq[String]] =
    filterService(serviceName) { service =>
      Search {
        search in indexName query {
          filteredQuery.filter {
            termsFilter(pattern.fields.serviceName.name, service)
          }
        } aggregations {
          aggregation terms "nameAgg" field pattern.fields.spanName.name
        }
      } map { res =>
        val agg = res.aggregations.getAsMap.get("nameAgg")
        agg.isInstanceOf[StringTerms] match {
          case true => agg.asInstanceOf[StringTerms].getBuckets.asScala.map {
            _.asInstanceOf[StringTerms.Bucket].getKey
          }
          case false => Seq.empty
        }
      }
    }

  def searchTracesByIds(traceIds: Seq[Long]): Search[Seq[Span]] =
    Search {
      search in indexName query {
        filteredQuery.filter {
          termsFilter(pattern.fields.traceId.name, traceIds.map {
            SpanId(_).toString
          }: _*)
        }
      }
    } map {
      _.as[Option[Span]].flatten.toSeq
    }

  def searchServiceNames: Search[Seq[ServiceName]] =
    Search {
      search in indexName aggregations {
        aggregation terms "serviceAgg" field pattern.fields.serviceName.name
      }
    } map { res =>
      val agg = res.aggregations.getAsMap.get("serviceAgg")
      agg.isInstanceOf[StringTerms] match {
        case true => agg.asInstanceOf[StringTerms].getBuckets.asScala.map { s =>
          pattern.fields.serviceName.fromIndex(s.asInstanceOf[StringTerms.Bucket].getKey)
        }
        case false => Seq.empty
      }
    }

}
