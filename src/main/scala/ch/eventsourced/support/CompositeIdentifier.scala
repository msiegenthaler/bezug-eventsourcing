package ch.eventsourced.support

import java.net.{URLDecoder, URLEncoder}
import scala.util.Try

/** Identifier that has a path-like structure. Can be parsed from and serialized to string. */
sealed trait CompositeIdentifier {
  def /(part: String): CompositeIdentifier
  def parent: CompositeIdentifier
  def serialize: String =
    CompositeIdentifier.separator + serializedParts.mkString(CompositeIdentifier.separator)
  def serializedParts: Seq[String]
  override def toString = serialize
}

object CompositeIdentifier {
  def parse(value: String): Option[CompositeIdentifier] = {
    if (value.isEmpty || !value.startsWith(separator)) None
    else Try {
      value.split(separator).
        map(URLDecoder.decode(_, charset)).
        filter(_.nonEmpty).
        foldLeft(CompositeIdentifier.root)(_ / _)
    }.toOption
  }

  private val separator = "/"
  private val charset = "UTF-8"

  val root: CompositeIdentifier = new CompositeIdentifier {
    def serializedParts = Nil
    def /(part: String) = CompositeIdentifierCons(this, part)
    def parent = this
  }


  private case class CompositeIdentifierCons(parent: CompositeIdentifier, part: String) extends CompositeIdentifier {
    def /(part: String) = CompositeIdentifierCons(this, part)
    def serializedParts = parent.serializedParts :+ URLEncoder.encode(part, charset)
  }
}