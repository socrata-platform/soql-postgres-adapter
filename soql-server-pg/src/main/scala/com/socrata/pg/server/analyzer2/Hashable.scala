package com.socrata.pg.server.analyzer2

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.sql.Debug

import com.socrata.pg.analyzer2.metatypes.Stage

trait Hashable[T] {
  def hash(hasher: Hasher, value: T): Unit

  // if "isString" is true, it's guaranteed that what will get fed into
  // the hasher is UTF-8 bytes followed by Hasher.stringTerminator
  def isString = false
}

object Hashable {
  implicit object boolean extends Hashable[Boolean] {
    override def hash(hasher: Hasher, value: Boolean) = hasher.hashByte(if(value) 1 else 0)
  }

  implicit object byte extends Hashable[Byte] {
    override def hash(hasher: Hasher, value: Byte) = hasher.hashByte(value)
  }

  implicit object int extends Hashable[Int] {
    override def hash(hasher: Hasher, value: Int) = hasher.hashInt(value)
  }

  implicit object long extends Hashable[Long] {
    override def hash(hasher: Hasher, value: Long) = hasher.hashLong(value)
  }

  implicit object bytes extends Hashable[Array[Byte]] {
    override def hash(hasher: Hasher, value: Array[Byte]) = hasher.hashBytes(value)
  }

  implicit object string extends Hashable[String] {
    override def hash(hasher: Hasher, value: String) = hasher.hashString(value)
    override def isString = true
  }

  implicit object bigInt extends Hashable[BigInt] {
    override def hash(hasher: Hasher, value: BigInt) = hasher.hashString(value.toString)
    override def isString = true
  }

  implicit def pair[A: Hashable, B: Hashable]: Hashable[(A, B)] =
    new Hashable[(A, B)] {
      override def hash(hasher: Hasher, xs: (A, B)) = {
        hasher.hash(xs._1)
        hasher.hash(xs._2)
      }
    }

  implicit def seq[T: Hashable]: Hashable[Seq[T]] =
    new Hashable[Seq[T]] {
      override def hash(hasher: Hasher, xs: Seq[T]) = {
        hasher.hashInt(xs.length)
        for(x <- xs) {
          hasher.hash(x)
        }
      }
    }

  implicit def map[K: Hashable: Ordering, V: Hashable]: Hashable[Map[K, V]] =
    new Hashable[Map[K, V]] {
      override def hash(hasher: Hasher, xs: Map[K, V]) = {
        hasher.hash(xs.toSeq.sortBy(_._1))
      }
    }

  implicit def option[T](implicit ev: Hashable[T]): Hashable[Option[T]] = {
    new Hashable[Option[T]] {
      override def hash(hasher: Hasher, value: Option[T]) = {
        value match {
          case Some(x) =>
            if(!ev.isString) hasher.hashByte(0)
            hasher.hash(x)
          case None =>
            hasher.hashByte(Hasher.nonStringByte)
        }
      }
    }
  }

  implicit object datasetInternalName extends Hashable[DatasetInternalName] {
    override def hash(hasher: Hasher, value: DatasetInternalName) = hasher.hashString(value.underlying)
    override def isString = true
  }

  implicit object userColumnId extends Hashable[UserColumnId] {
    override def hash(hasher: Hasher, value: UserColumnId) = hasher.hashString(value.underlying)
    override def isString = true
  }

  implicit def databaseTableName[T](implicit ev: Hashable[T]): Hashable[DatabaseTableName[T]] =
    new Hashable[DatabaseTableName[T]] {
      override def hash(hasher: Hasher, value: DatabaseTableName[T]) = hasher.hash(value.name)
      override def isString = ev.isString
    }

  implicit def databaseColumnName[T](implicit ev: Hashable[T]): Hashable[DatabaseColumnName[T]] =
    new Hashable[DatabaseColumnName[T]] {
      override def hash(hasher: Hasher, value: DatabaseColumnName[T]) = hasher.hash(value.name)
      override def isString = ev.isString
    }

  implicit object rewritePass extends Hashable[rewrite.Pass] {
    override def hash(hasher: Hasher, value: rewrite.Pass) =
      value match {
        case rewrite.Pass.InlineTrivialParameters => hasher.hashByte(0)
        case rewrite.Pass.PreserveOrdering => hasher.hashByte(1)
        case rewrite.Pass.RemoveTrivialSelects => hasher.hashByte(2)
        case rewrite.Pass.ImposeOrdering => hasher.hashByte(3)
        case rewrite.Pass.Merge => hasher.hashByte(4)
        case rewrite.Pass.RemoveUnusedColumns => hasher.hashByte(5)
        case rewrite.Pass.RemoveUnusedOrderBy => hasher.hashByte(6)
        case rewrite.Pass.UseSelectListReferences => hasher.hashByte(7)
        case rewrite.Pass.Page(size, off) =>
          hasher.hashByte(8)
          hasher.hash(size)
          hasher.hash(off)
        case rewrite.Pass.AddLimitOffset(lim, off) =>
          hasher.hashByte(9)
          hasher.hash(lim)
          hasher.hash(off)
        case rewrite.Pass.RemoveOrderBy => hasher.hashByte(10)
      }
  }

  implicit object columnName extends Hashable[ColumnName] {
    override def hash(hasher: Hasher, value: ColumnName) = hasher.hash(value.name)
    override def isString = true
  }

  implicit object debug extends Hashable[Debug] {
    private implicit val sqlFormat = new Hashable[Debug.Sql.Format] {
      override def hash(hasher: Hasher, value: Debug.Sql.Format) = {
        value match {
          case Debug.Sql.Format.Compact => hasher.hash(0)
          case Debug.Sql.Format.Pretty => hasher.hash(1)
        }
      }
    }

    private implicit val explain = new Hashable[Debug.Explain] {
      private implicit val explainFormat = new Hashable[Debug.Explain.Format] {
        override def hash(hasher: Hasher, value: Debug.Explain.Format) = {
          value match {
            case Debug.Explain.Format.Text => hasher.hash(0)
            case Debug.Explain.Format.Json => hasher.hash(1)
          }
        }
      }

      override def hash(hasher: Hasher, value: Debug.Explain) = {
        val Debug.Explain(analyze, format) = value
        hasher.hash(analyze)
        hasher.hash(format)
      }
    }

    override def hash(hasher: Hasher, value: Debug) = {
      val Debug(sql, explainSpec, inhibitRun) = value
      hasher.hash(sql)
      hasher.hash(explainSpec)
      hasher.hash(inhibitRun)
    }
  }

  implicit object stage extends Hashable[Stage] {
    override def hash(hasher: Hasher, value: Stage) = hasher.hashString(value.underlying)
    override def isString = true
  }
}
