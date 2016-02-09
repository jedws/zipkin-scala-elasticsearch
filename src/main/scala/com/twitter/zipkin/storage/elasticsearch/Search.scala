package com.twitter.zipkin.storage.elasticsearch

import com.sksamuel.elastic4s.SearchDefinition
import org.elasticsearch.action.search.SearchResponse

sealed trait Search[R] {
  import Search._

  def map[S](f: R => S): Search[S] =
    fold(f andThen Result.apply, { (query, filter) => Query(query, filter andThen f) })

  def fold[X](result: R => X, query: (SearchDefinition, SearchResponse => R) => X): X =
    this match {
      case Result(r) => result(r)
      case Query(q, f) => query(q, f)
    }
}

object Search {
  def apply(query: SearchDefinition): Search[SearchResponse] =
    Query(query, identity)

  def ok[R](r: R): Search[R] =
    Result(r)

  case class Result[R](value: R) extends Search[R]
  case class Query[R](query: SearchDefinition, filter: SearchResponse => R) extends Search[R]
}
