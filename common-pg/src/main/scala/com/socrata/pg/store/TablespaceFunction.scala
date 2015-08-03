package com.socrata.pg.store

object TablespaceFunction {
  def apply(tablespace: Option[String]): (String => Option[String]) = {
    tablespace match {
      case Some(fn) =>
        // force RT to be initialized to avoid circular-dependency NPE
        // Merely putting a reference to T is sufficient; the call to hashCode
        // is to silence a compiler warning about ignoring a pure value
        clojure.lang.RT.T.hashCode
        val iFn = clojure.lang.Compiler.load(
          new java.io.StringReader(
          s"""(let [op (fn [^String table-name] ${fn})]
             |  (fn [^String table-name]
             |    (let [result (op table-name)]
             |      (if result
             |        (scala.Some. result)
             |        (scala.Option/empty)))))
           """.stripMargin)).asInstanceOf[clojure.lang.IFn]
        (t: String) => iFn.invoke(t).asInstanceOf[Option[String]]
      case None =>
        (t: String) => None
    }
  }
}
