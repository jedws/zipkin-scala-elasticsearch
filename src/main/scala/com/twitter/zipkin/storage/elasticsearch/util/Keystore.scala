package com.twitter.zipkin.storage.elasticsearch
package util

import java.nio.file._

case class Keystore(resourceName: String) {
  private val in = getClass.getResourceAsStream(resourceName)

  val path = Files.createTempFile("keystore", "tmp")

  Files.copy(in, path, StandardCopyOption.REPLACE_EXISTING)

  def close() = Files.delete(path)
}
