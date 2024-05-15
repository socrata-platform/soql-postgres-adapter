import java.io.{FileInputStream, InputStreamReader, FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import sbt._

import com.rojoma.simplearm.v2._

object BuildRustStoredProcs {
  def apply(dynamicResourceDir: File, staticResourceDir: File, rustDir: File): Seq[File] = {
    val bf = readFile(rustDir / "blowfish.rs")
    val formatter = readFile(rustDir / "longformatter.rs")

    val source = readFile(staticResourceDir / "com" / "socrata" / "pg" / "store" / "schema" / "20240510-id-version-obfuscation.xml.in")

    val outFile = dynamicResourceDir / "com" / "socrata" / "pg" / "store" / "schema" / "20240510-id-version-obfuscation.xml"
    outFile.getParentFile.mkdirs()

    writeFile(outFile, source.replaceAll("%%INCLUDE_BLOWFISH%%", bf).replaceAll("%%INCLUDE_LONGFORMATTER%%", formatter))

    Seq(outFile)
  }

  private def readFile(f: File): String = {
    for {
      fis <- managed(new FileInputStream(f))
      isr <- managed(new InputStreamReader(fis, StandardCharsets.UTF_8))
    } {
      val result = new StringBuilder
      val buf = new Array[Char](10240)

      def loop(): Unit = {
        isr.read(buf) match {
          case -1 =>
            // done
          case n =>
            result.appendAll(buf, 0, n)
            loop()
        }
      }
      loop()

      result.toString
    }
  }

  private def writeFile(f: File, text: String): Unit = {
    for {
      fos <- managed(new FileOutputStream(f))
      isw <- managed(new OutputStreamWriter(fos, StandardCharsets.UTF_8))
    } {
      isw.write(text)
    }
  }
}
