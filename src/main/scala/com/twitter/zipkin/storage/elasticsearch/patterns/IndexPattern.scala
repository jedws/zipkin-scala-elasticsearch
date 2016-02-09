package com.twitter.zipkin.storage.elasticsearch.patterns

import java.util.Date

import com.twitter.zipkin.common._

trait IndexPattern {
  val fields: Fields
  def apply(date: Date): String
  def decodeSpan(json: String): Option[Span]
  def translateBinaryAnnotation(name: String): Option[String]
}

case class ServiceName(self: String) extends Proxy

case class Fields(
  serviceName: Field[ServiceName],
  spanName: Field[String],
  traceId: Field[Long]
)

case class Field[A](name: String, toIndex: A => Option[String], fromIndex: String => A)

object Field {
  def simple(name: String): Field[String] =
    Field(name, Some.apply, identity)
}
