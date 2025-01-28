package com.socrata.pg.server.analyzer2

import java.util.ArrayList

import org.slf4j.LoggerFactory

// A priority queue where the entries know their own positions in the
// queue in order to allow for O(log n) removal and priority-change.
class IntrusivePriorityQueue[T <: IntrusivePriorityQueue.Mixin](private val cmp: (T#Priority, T#Priority) => Int) {
  import IntrusivePriorityQueue.log

  private val items = new ArrayList[T]

  override def toString = items.toString

  def insert(value: T)(priority: value.Priority): Unit = {
    if(value.queue ne null) {
      throw new IllegalStateException("Value is already in another queue")
    }

    value.queue = this
    value.index = items.size
    value.priority = priority
    items.add(value)

    heapDown(value)
  }

  def isEmpty = items.size == 0

  def remove(value: T): Boolean = {
    doRemove(value).isDefined
  }

  private def doRemove(value: T): Option[T] = {
    if(value.queue ne this) {
      None
    } else {
      if(value.index < items.size - 1) {
        val oldEnd = items.get(items.size - 1)
        swap(value, oldEnd)
        items.remove(value.index)
        heapify(oldEnd)
      } else {
        items.remove(value.index)
      }

      value.queue = null
      value.index = -1
      Some(value)
    }
  }

  private def heapify(value: T): Unit = {
    // We've just moved an item into an arbitrary place in the tree;
    // we need to restore the heap property.
    if(value.index != 0) {
      val parentIdx = value.index / 2
      if(cmp(items.get(parentIdx).priority, value.priority) > 0) {
        log.debug("{} is bigger than {}, need to heap-down", items.get(parentIdx).priority, value.priority)
        heapDown(value)
      } else {
        log.debug("Not bigger than its parent, heaping it up")
        heapUp(value)
      }
    } else {
      log.debug("Has no parent, heaping it up")
      heapUp(value)
    }
  }

  def peek: Option[(T, T#Priority)] =
    if(items.size > 0) {
      val value = items.get(0)
      Some((value, value.priority))
    } else {
      None
    }

  def pop(): Option[T] =
    if(items.size > 0) {
      doRemove(items.get(0))
    } else {
      None
    }

  def popIf(pred: (T, T#Priority) => Boolean): Option[T] =
    if(items.size > 0) {
      val v = items.get(0)
      if(pred(v, v.priority)) {
        doRemove(items.get(0))
      } else {
        None
      }
    } else {
      None
    }

  def heapCheck(): Unit = {
    if(!isEmpty) {
      for(i <- 0 until (items.size / 2)) {
        val leftChildIdx = i*2
        val rightChildIdx = leftChildIdx + 1
        assert(cmp(items.get(i).priority, items.get(leftChildIdx).priority) <= 0, s"$i (${items.get(i).priority}) was bigger than $leftChildIdx (${items.get(leftChildIdx).priority})")
        if(rightChildIdx < items.size) {
          assert(cmp(items.get(i).priority, items.get(rightChildIdx).priority) <= 0, s"$i (${items.get(i).priority}) was bigger than $rightChildIdx (${items.get(rightChildIdx).priority})")
        }
      }
    }
  }

  private def heapDown(value: T): Unit = {
    while(value.index > 0) {
      val parentIdx = value.index / 2;
      if(cmp(items.get(parentIdx).priority, value.priority) > 0) {
        swap(value, items.get(parentIdx))
      } else {
        return
      }
    }
  }

  private def heapUp(value: T): Unit = {
    log.debug("HeapUp from {} ({})", value.priority, value.index)
    while(value.index * 2 < items.size) {
      log.debug("Looking at {} ({})", value.priority, value.index)
      val leftChildIdx = value.index * 2;
      val rightChildIdx = leftChildIdx + 1;
      if(cmp(value.priority, items.get(leftChildIdx).priority) > 0) {
        // it's bigger than the left child
        log.debug("{} is bigger than left child {}", value.priority, items.get(leftChildIdx).priority)
        if(rightChildIdx < items.size) {
          if(cmp(value.priority, items.get(rightChildIdx).priority) > 0) {
            // also bigger than the right child; swap it with
            // whichever of those two is smaller.
            log.debug("{} is bigger than right child {}", value.priority, items.get(rightChildIdx).priority)
            if(cmp(items.get(leftChildIdx).priority, items.get(rightChildIdx).priority) < 0) {
              log.debug(s"swapping left")
              swap(value, items.get(leftChildIdx))
            } else {
              log.debug(s"swapping right")
              swap(value, items.get(rightChildIdx))
            }
          } else {
            // left <= item <= right - swap into the left branch
            log.debug("swapping left")
            swap(value, items.get(leftChildIdx))
          }
        } else {
          log.debug("there is no right child; swapping left")
          swap(value, items.get(leftChildIdx))
        }
      } else {
        // it's smaller than the left child
        log.debug("{} is smaller than left child {}", value.priority, items.get(leftChildIdx).priority)
        if(rightChildIdx < items.size) {
          if(cmp(value.priority, items.get(rightChildIdx).priority) > 0) {
            // right <= item <= left
            log.debug("{} is bigger than right child {}; swapping right", value.priority, items.get(rightChildIdx).priority)
            swap(value, items.get(rightChildIdx))
          } else {
            // smaller than both; we're done
            log.debug("{} is smaller than both (right is {}); done", value.priority, items.get(rightChildIdx).priority)
            return
          }
        } else {
          // there is no right child; we're done
          log.debug(s"No right child; done")
          return
        }
      }
    }
    log.debug("Reached the end; done")
  }

  def adjustPriority(value: T)(newPriority: value.Priority): Unit = {
    if(value.queue ne this) return

    val change = cmp(value.priority, newPriority)
    value.priority = newPriority
    if(change > 0) {
      // old priority was bigger than the new priority; it might now
      // be smaller than its parent.
      heapDown(value)
    } else if(change < 0) {
      heapUp(value)
    }
  }

  private def swap(a: T, b: T): Unit = {
    items.set(b.index, a)
    items.set(a.index, b)
    val tmp = b.index
    b.index = a.index
    a.index = tmp
  }
}

object IntrusivePriorityQueue {
  private val log = LoggerFactory.getLogger(classOf[IntrusivePriorityQueue[_]])

  trait Mixin {
    type Priority

    private[IntrusivePriorityQueue] var queue: IntrusivePriorityQueue[_] = null
    private[IntrusivePriorityQueue] var index: Int = -1;
    private[IntrusivePriorityQueue] var priority: Priority = _;
  }
}
