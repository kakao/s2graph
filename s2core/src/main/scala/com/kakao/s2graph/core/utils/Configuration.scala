package com.kakao.s2graph.core.utils

import com.typesafe.config.Config

import scala.language.implicitConversions

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 12. 8..
  */
object Configuration {
  implicit def configToConfiguration(conf: Config): Configuration = {
    new Configuration(conf)
  }
}

class Configuration(underlying: Config) {
  def getOrElse[T](key:String, default: T): T = {
    val ret = if (underlying.hasPath(key)) (default match {
      case _: String => underlying.getString(key)
      case _: Int | _: java.lang.Integer => underlying.getInt(key)
      case _: Long | _: java.lang.Long => underlying.getLong(key)
      case _: Float | _: Double => underlying.getDouble(key)
      case _: Boolean => underlying.getBoolean(key)
      case _ => default
    }).asInstanceOf[T]
    else default
    println(s"${this.getClass.getName}: $key -> $ret")
    ret
  }
}
