package com.socrata.pg.store.events

import java.io.Closeable
import java.nio.charset.StandardCharsets

import com.rojoma.simplearm.v2.ResourceScope
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCacheListener, NodeCache}

trait RowsChangedPreviewConfig {
  def minRows(columnCount: Int): Long
  def fractionOf(columnCount: Int): Double
}

object RowsChangedPreviewConfig {
  class Clojure(val minRowsSource: String, val fractionOfSource: String) extends RowsChangedPreviewConfig {
    clojure.lang.RT.T.hashCode(); // force RT to be initialized
    val minRowsFunc = clojure.lang.Compiler.load(new java.io.StringReader(minRowsSource)).asInstanceOf[clojure.lang.IFn]
    val fractionOfFunc = clojure.lang.Compiler.load(new java.io.StringReader(fractionOfSource)).asInstanceOf[clojure.lang.IFn]

    override def minRows(columnCount: Int): Long =
      minRowsFunc.invoke(columnCount).asInstanceOf[java.lang.Number].longValue()

    override def fractionOf(columnCount: Int): Double =
      fractionOfFunc.invoke(columnCount).asInstanceOf[java.lang.Number].doubleValue()
  }

  object Default extends Clojure("(fn [cols] 100000)", "(fn [cols] 0.33333)")

  class ZKClojure(curator: CuratorFramework, path: String) extends RowsChangedPreviewConfig with Closeable {
    private val resourceScope = new ResourceScope
    private val updateMutex = new Object
    private val minRowsCache = resourceScope.open(new NodeCache(curator, path + "/min-rows-func"))
    private val fractionOfCache = resourceScope.open(new NodeCache(curator, path + "/fraction-of-func"))

    @volatile private var current: Clojure = Default

    private val minRowsListener = new NodeCacheListener {
      override def nodeChanged(): Unit = {
        minRowsCache.getCurrentData match {
          case null =>
            updateMutex.synchronized {
              current = new Clojure(Default.minRowsSource, current.fractionOfSource)
            }
          case newSource =>
            updateMutex.synchronized {
              current = new Clojure(new String(newSource.getData, StandardCharsets.UTF_8), current.fractionOfSource)
            }
        }
      }
    }

    private val fractionOfListener = new NodeCacheListener {
      override def nodeChanged(): Unit = {
        fractionOfCache.getCurrentData match {
          case null =>
            updateMutex.synchronized {
              current = new Clojure(current.minRowsSource, Default.fractionOfSource)
            }
          case newSource =>
            updateMutex.synchronized {
              current = new Clojure(current.minRowsSource, new String(newSource.getData, StandardCharsets.UTF_8))
            }
        }
      }
    }

    minRowsCache.getListenable.addListener(minRowsListener)
    fractionOfCache.getListenable.addListener(fractionOfListener)

    def close(): Unit = {
      resourceScope.close()
    }

    def start(): Unit = {
      minRowsCache.start(true)
      try {
        fractionOfCache.start(true)
      } catch {
        case t: Throwable =>
          try {
            resourceScope.close()
          } catch {
            case t2: Throwable =>
              t.addSuppressed(t2)
          }
          throw t
      }
    }

    override def minRows(columnCount: Int): Long = current.minRows(columnCount)

    override def fractionOf(columnCount: Int): Double = current.fractionOf(columnCount)
  }
}
