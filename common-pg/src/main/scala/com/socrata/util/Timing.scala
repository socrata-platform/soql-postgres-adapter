package com.socrata.util

import scala.concurrent.duration.{Duration, MILLISECONDS}

object Timing {

  def wrap[T](block: =>T)(fun:(T,Duration)=>Unit): T ={
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    fun(result,Duration(end-start,MILLISECONDS))
    result
  }
}
