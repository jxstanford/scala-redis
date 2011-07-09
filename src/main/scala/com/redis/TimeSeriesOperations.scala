package com.redis

import serialization._

trait TimeSeriesOperations { self: Redis =>
  
  // TSLEN
  //
  def tslen(key: Any)(implicit format: Format): Option[Int] = {
    send("TSLEN", List(key))(asInt) match {
      case Some(0) => None
      case Some(x:Int) => Some(x)
      case _ => None
    }
  }

  // TSADD
  // Add the specified value  having the specified time to the time series stored at key.  Returns the number of records 
  // added.  
  // TODO TSADD takes multipe time/value pairs.   
  def tsadd(key: Any, time: Double, value: Any)(implicit format: Format): Option[Int]  = {
    send("TSADD", List(key, time, value))(asInt) match {
      case Some(0) => None
      case Some(x:Int) => Some(x)
      case _ => None
    }
  }

  def tsadd(key: Any, timeValue: Pair[Double,Any]*)(implicit format:Format): Option[Int] = {
    send("TSADD", List(key :: timeValue.toList.flatMap(tv => List(tv._1, tv._2))).flatten)(asInt) match {
      case Some(0) => None
      case Some(x:Int) => Some(x)
      case _ => None
    }
  }
 
  // TSEXISTS
  //
  def tsexists(key: Any, time: Double)(implicit format: Format): Boolean = 
    send("TSEXISTS", List(key, time))(asBoolean)

  // TSGET
  // 
  def tsget[A](key: Any, time: Double)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("TSGET", List(key, time))(asBulk)

  // TSRANGE
  // 
  def tsrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    send("TSRANGE", List(key, start, end))(asList.map(_.flatten))

  // TSRANGE WITHTIMES
  // Brings back time/value pairs for entries within the range.  The range includes start and end which
  // represent positions in the series.
  def tsrangeWithTimes[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[List[(Double, A)]] = {
    send("TSRANGE", List(key, start, end, "withtimes"))(asListPairs(Parse.Implicits.parseDouble, parse).map(_.flatten))
  }
  // TSRANGE NOVALUES
  // Brings back time entries within the range.  The range includes start and end which
  // represent positions in the series.
  def tsrangeNoValues(key: Any, start: Int, end: Int)(implicit format: Format): Option[List[Double]] =
    send("TSRANGE", List(key, start, end, "novalues"))(asList[Double](Parse.Implicits.parseDouble).map(_.flatten))

  // TSRANGEBYTIME
  //
  def tsrangebytime[A](key: Any, start: Double, end: Double)(implicit format: Format, parse: Parse[A]): Option[List[A]] =
    send("TSRANGEBYTIME", List(key, start, end))(asList.map(_.flatten))

  // TSRANGEBYTIME WITHTIMES
  // Brings back time/value pairs for entries within the range.  The range includes start and end which
  // represent positions in the series.
  def tsrangebytimeWithTimes[A](key: Any, start: Double, end: Double)(implicit format: Format, parse: Parse[A]): Option[List[(Double, A)]] = {
    send("TSRANGEBYTIME", List(key, start, end, "withtimes"))(asListPairs(Parse.Implicits.parseDouble, parse).map(_.flatten))
  }

// TSRANGE NOVALUES
  // Brings back time entries within the range.  The range includes start and end which
  // represent positions in the series.
  def tsrangebytimeNoValues(key: Any, start: Double, end: Double)(implicit format: Format): Option[List[Double]] =
    send("TSRANGEBYTIME", List(key, start, end, "novalues"))(asList[Double](Parse.Implicits.parseDouble).map(_.flatten))

  // TSCOUNT
  //
  def tscount(key: Any, startTime: Double, endTime: Double)(implicit format: Format): Option[Int]  = {
    send("TSCOUNT", List(key, startTime, endTime))(asInt) match {
      case Some(0) => None
      case Some(x:Int) => Some(x)
      case _ => None
    }
  }
}
