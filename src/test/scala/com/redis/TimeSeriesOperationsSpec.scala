package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class TimeSeriesOperationsSpec extends Spec
                        with ShouldMatchers
                        with BeforeAndAfterEach
                        with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)

  override def beforeAll = {
    r.flushdb
  }

  override def beforeEach = {
    r.flushdb
  }

  override def afterEach = {}

  override def afterAll = {
    r.disconnect
  }

  import r._

  private def add = {
    tsadd("esu1", 1, "1300.00,10,1300.00,1300.25") should equal(Some(1))
    tsadd("esu1", 2, "1299.75,1,1299.75,1300.00") should equal(Some(1))
    tsadd("esu1", 3, "1299.50,5,1299.50,1299.75") should equal(Some(1))
    tsadd("esu1", 4, "1299.25,10,1299.25,1299.50") should equal(Some(1))
    tsadd("esu1", 5, "1299.0,100,1299.0,1299.25") should equal(Some(1))
    tsadd("esu1", (6.toDouble, "1299.00,99,1299.00,1299.25"), (7.toDouble, "1299.25,98,1299.25,1299.50")) should equal (Some(2))
  }

  describe("tsadd") {
    it("should add based on proper time series semantics") {
      add
      tsadd("esu1",1,"1300.00,10,1300.0,1300.25") should equal(None)
      tslen("esu1") should equal(Some(7))
      tsexists("esu1", 1) should equal(true)
      tsexists("esu1", 3) should equal(true)
      tsexists("esu1", 5) should equal(true)
      tsexists("esu1", 6) should equal(true)
      tsexists("esu1", 7) should equal(true)
    }
  }

  describe("tsget") {
    it("should retrieve the proper value") {
      add
      tsget[String]("esu1", 1).isDefined should equal (true)
      tsget[String]("esu1", 2).isDefined should equal (true)
      tsget[String]("esu1", 2).get should equal ("1299.75,1,1299.75,1300.00")
      tsget[String]("esu1", 3).isDefined should equal (true)
      tsget[String]("esu1", 4).isDefined should equal (true)
      tsget[String]("esu1", 4).get should equal ("1299.25,10,1299.25,1299.50")
      tsget[String]("esu1", 5).isDefined should equal (true)
      tsget[String]("esu1", 6).isDefined should equal (true)
      tsget[String]("esu1", 6).get should equal ("1299.00,99,1299.00,1299.25")
      tsget[String]("esu1", 7).isDefined should equal (true)
      tsget[String]("esu1", 7).get should equal ("1299.25,98,1299.25,1299.50")
    }
  }

  describe("tslen") {
    it("should return the length of the store for key") {
        add
        tslen("esu1").isDefined should equal (true)
        tslen("esu1").get should equal (7)
    }
  }

  describe("tsrange with no flags") {
    it("should retrieve values") {
      add
      val r1 = tsrange[String]("esu1", 0, 4)
      r1.isDefined should equal (true)
      r1.get.size should equal (5)
      r1.get.head should equal ("1300.00,10,1300.00,1300.25")
      r1.get.last should equal ("1299.0,100,1299.0,1299.25")
      val r2 = tsrange[String]("esu1", 5, 6)
      r2.isDefined should equal (true)
      r2.get.size should equal (2)
      r2.get.head should equal ("1299.00,99,1299.00,1299.25")
      r2.get.last should equal ("1299.25,98,1299.25,1299.50")
      val r3 = tsrange[String]("esu1", 0, 100)
      r3.isDefined should equal (true)
      r3.get.size should equal (7)
    }
  }

  describe("tsrange with 'withtimes' flag") {
      it ("should retrieve times and values") {
        add
        val r1 = tsrangeWithTimes[String]("esu1", 0, 4)
        r1.isDefined should equal (true)
        r1.get.size should equal (5)
        r1.get.head should equal ((1,"1300.00,10,1300.00,1300.25"))
        r1.get.apply(4) should equal ((5,"1299.0,100,1299.0,1299.25"))
      }
  }

  describe("tsrange with 'novalues' flag") {
    it ("should retrieve values") {
      add
      val r1 = tsrangeNoValues("esu1", 0, 4)
      r1.isDefined should equal (true)
      r1.get.size should equal (5)
      r1.get.head should equal (1)
      r1.get.apply(4) should equal (5)
    }
  }

  describe("tsrangebytime with no flags") {
    it("should retrieve values") {
      add
      val r1 = tsrangebytime[String]("esu1", 1, 5)
      r1.isDefined should equal (true)
      r1.get.size should equal (5)
      r1.get.head should equal ("1300.00,10,1300.00,1300.25")
      r1.get.last should equal ("1299.0,100,1299.0,1299.25")
      val r2 = tsrangebytime[String]("esu1", 6, 7)
      r2.isDefined should equal (true)
      r2.get.size should equal (2)
      r2.get.head should equal ("1299.00,99,1299.00,1299.25")
      r2.get.last should equal ("1299.25,98,1299.25,1299.50")
      val r3 = tsrangebytime[String]("esu1", 0, 100)
      r3.isDefined should equal (true)
      r3.get.size should equal (7)
    }
  }

  describe("tsrangebytime with 'withtimes' flag") {
    it ("should retrieve times and values") {
      add
      val r1 = tsrangebytimeWithTimes[String]("esu1", 1, 5)
      r1.isDefined should equal (true)
      r1.get.size should equal (5)
      r1.get.head should equal ((1,"1300.00,10,1300.00,1300.25"))
      r1.get.apply(4) should equal ((5,"1299.0,100,1299.0,1299.25"))
    }
  }

  describe("tsrangebytime with 'novalues' flag") {
    it ("should retrieve values") {
      add
      val r1 = tsrangebytimeNoValues("esu1", 1, 5)
      r1.isDefined should equal (true)
      r1.get.size should equal (5)
      r1.get.head should equal (1)
      r1.get.apply(4) should equal (5)
    }
  }
}
