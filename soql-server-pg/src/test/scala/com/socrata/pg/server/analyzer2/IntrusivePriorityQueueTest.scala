package com.socrata.pg.server.analyzer2

import scala.util.Random

import org.scalatest.{FunSuite, MustMatchers}

class IntrusivePriorityQueueTest extends FunSuite with MustMatchers {
  case class Value(var x: Int) extends IntrusivePriorityQueue.Mixin {
    type Priority = Int
  }

  test("Heapsort") {
    val q = new IntrusivePriorityQueue[Value](_.compareTo(_))
    val items = Vector.newBuilder[Int]

    for(i <- 0 until 1000) {
      val x = Random.nextInt()
      q.insert(Value(x))(x)
      items += x
      q.heapCheck()
    }

    val expected = items.result().sorted

    def loop(it: Iterator[Int]) {
      q.isEmpty must be (!it.hasNext)
      if(it.hasNext) {
        q.pop().map(_.x) must be (Some(it.next()))
        q.heapCheck()
        loop(it)
      }
    }
    loop(expected.iterator)
  }

  test("Modifying entries") {
    val q = new IntrusivePriorityQueue[Value](_.compareTo(_))
    val items = Vector.newBuilder[Value]

    for(i <- 0 until 1000) {
      val x = Random.nextInt()
      val v = Value(x)
      q.insert(v)(x)
      items += v
      q.heapCheck()
    }

    var expected = items.result()

    for(i <- 0 until 100) {
      val idx = Random.nextInt(expected.length)
      val newValue = Random.nextInt()
      expected(idx).x = newValue
      q.adjustPriority(expected(idx))(newValue)
    }

    expected = expected.sortBy(_.x)

    def loop(it: Iterator[Value]) {
      q.isEmpty must be (!it.hasNext)
      if(it.hasNext) {
        q.pop().map(_.x) must be (Some(it.next().x))
        q.heapCheck()
        loop(it)
      }
    }
    loop(expected.iterator)
  }

  test("Removing entries") {
    val q = new IntrusivePriorityQueue[Value](_.compareTo(_))
    val items = Vector.newBuilder[Value]

    for(i <- 0 until 1000) {
      val x = Random.nextInt()
      val v = Value(x)
      q.insert(v)(x)
      items += v
      q.heapCheck()
    }

    var expected = items.result()

    for(i <- 0 until 100) {
      q.remove(expected(i)) must be (true)
      q.heapCheck()
    }

    expected = expected.drop(100).sortBy(_.x)

    def loop(it: Iterator[Value]) {
      q.isEmpty must be (!it.hasNext)
      if(it.hasNext) {
        q.pop().map(_.x) must be (Some(it.next().x))
        q.heapCheck()
        loop(it)
      }
    }
    loop(expected.iterator)
  }
}
