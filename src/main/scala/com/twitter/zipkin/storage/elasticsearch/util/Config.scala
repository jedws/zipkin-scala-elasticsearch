package com.twitter.zipkin.storage.elasticsearch
package util

import org.elasticsearch.common.settings.ImmutableSettings

case class TrustStore(resource: String, password: String) {
  lazy val keystore = Keystore(resource)
}

object TrustStore {
  def apply: TrustStore = TrustStore("", "")
}

case class ShieldConfig(username: String, password: String, ssl: Option[ShieldSSL]) {
  def configure(b: ImmutableSettings.Builder): ImmutableSettings.Builder = {
    b.put("shield.user", s"$username:$password")
    ssl.foreach { s =>
      b.put("shield.ssl.hostname_verification", s.hostnameVerification)
      b.put("shield.ssl.truststore.path", s.trustStore.keystore.path)
      b.put("shield.ssl.truststore.password", s.trustStore.password)
      b.put("shield.transport.ssl", true)
    }
    b
  }
}

case class ShieldSSL(hostnameVerification: Boolean, trustStore: TrustStore)

case class Config(
   host: String,
   portOption: Option[Int],
   clientTransportSniff: Boolean,
   clusterName: String,
   shield: Option[ShieldConfig]
) {
  val port = portOption getOrElse 9300

  lazy val settings = {
    val builder = ImmutableSettings.settingsBuilder()
      .put("cluster.name", clusterName)
      .put("client.transport.sniff", clientTransportSniff)
    shield.foreach { _.configure(builder) }
    builder.build()
  }
}
