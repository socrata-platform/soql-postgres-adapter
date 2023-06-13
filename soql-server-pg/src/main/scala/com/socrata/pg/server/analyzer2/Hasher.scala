package com.socrata.pg.server.analyzer2

import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

trait Hasher {
  def hashByte(b: Byte): Unit
  def hashInt(n: Int): Unit
  def hashLong(n: Long): Unit
  def hashBytes(bs: Array[Byte], off: Int, len: Int): Unit
  final def hashBytes(bs: Array[Byte]): Unit = hashBytes(bs, 0, bs.length)
  def hashString(s: String): Unit

  def hash[T](x: T)(implicit ev: Hashable[T]): Unit = ev.hash(this, x)
}

trait HashComplete {
  def digest(): Array[Byte]
}

object Hasher {
  val stringTerminator = 0xff.toByte // Neither of these bytes
  val nonStringByte = 0xfe.toByte    // appear in valid UTF-8

  def newSha256(): Hasher with HashComplete =
    new MDHasher(MessageDigest.getInstance("SHA-256"))

  private class MDHasher(md: MessageDigest) extends Hasher with HashComplete {
    private val buf = ByteBuffer.allocate(8)

    override def hashByte(b: Byte) = md.update(b)

    override def hashInt(n: Int) = {
      buf.clear()
      buf.asIntBuffer.put(n)
      md.update(buf)
    }

    override def hashLong(n: Long) = {
      buf.clear()
      buf.asLongBuffer.put(n)
      md.update(buf)
    }

    override def hashBytes(bs: Array[Byte], off: Int, len: Int) = {
      hashInt(len)
      md.update(bs, off, len)
    }

    override def hashString(s: String) = {
      md.update(s.getBytes(StandardCharsets.UTF_8))
      md.update(stringTerminator)
    }

    override def digest() =
      md.digest()
  }
}
