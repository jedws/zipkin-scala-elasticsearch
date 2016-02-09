package com.twitter.zipkin.storage.elasticsearch

class FutureActionListener[A]
  extends com.twitter.util.Promise[A]
     with org.elasticsearch.action.ActionListener[A] {

  def onFailure(e: Throwable) = setException(e)
  def onResponse(a: A) = setValue(a)
}

object FutureActionListener {
  def apply[A]() = new FutureActionListener[A]
}
