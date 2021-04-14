package com.socrata.pg.store.events

trait RowsChangedPreviewConfig {
  def shouldCopy(inserts: Long, updates: Long, deletes: Long, columnCount: Long, estimatedExistingRowCount: Long): Boolean
}

object RowsChangedPreviewConfig {
  class Clojure(val source: String) extends RowsChangedPreviewConfig {
    clojure.lang.RT.T.hashCode(); // force RT to be initialized
    val shouldCopyFunc = clojure.lang.Compiler.load(new java.io.StringReader(s"""
      (fn [inserts updates deletes columns estimated-rows]
        $source)
    """)).asInstanceOf[clojure.lang.IFn]

    override def shouldCopy(inserts: Long, updates: Long, deletes: Long, columnCount: Long, estimatedExistingRowCount: Long): Boolean =
      shouldCopyFunc.invoke(inserts, updates, deletes, columnCount, estimatedExistingRowCount).asInstanceOf[java.lang.Boolean].booleanValue()
  }

  val default = """
    (let [total (+ inserts updates deletes)]
      (> total (max 100000 (/ estimated-rows 3))))
   """
}
