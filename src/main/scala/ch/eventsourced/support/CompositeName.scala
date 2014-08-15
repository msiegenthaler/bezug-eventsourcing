package ch.eventsourced.support

import java.net.{URLDecoder, URLEncoder}
import scala.util.Try

/** Identifier that has a path-like structure. Can be parsed from and serialized to string. */
sealed trait CompositeName {
  def /(part: String): CompositeName
  def /(next: CompositeName): CompositeName = next appendTo this
  protected def appendTo(other: CompositeName): CompositeName
  def parent: CompositeName
  def serialize: String =
    CompositeName.separator + serializedParts.mkString(CompositeName.separator)
  def serializedParts: Seq[String]
  override def toString = serialize
}

object CompositeName {
  def apply(value: String) = CompositeName.root / value
  def unapplySeq(identifier: CompositeName): Option[Seq[String]] = Some(identifier.serializedParts)
  def unapplySeq(value: String): Option[Seq[String]] = parse(value).flatMap(unapplySeq)

  val root: CompositeName = new CompositeName {
    def serializedParts = Nil
    def /(part: String) = CompositeNameCons(this, part)
    def appendTo(other: CompositeName) = other
    def parent = this
  }

  def parse(value: String): Option[CompositeName] = {
    if (value.isEmpty || !value.startsWith(separator)) None
    else Try {
      value.split(separator).
        map(URLDecoder.decode(_, charset)).
        filter(_.nonEmpty).
        foldLeft(CompositeName.root)(_ / _)
    }.toOption
  }

  private val separator = "/"
  private val charset = "UTF-8"
  private case class CompositeNameCons(parent: CompositeName, part: String) extends CompositeName {
    def /(part: String) = CompositeNameCons(this, part)
    def appendTo(other: CompositeName) = parent.appendTo(other) / part
    def serializedParts = parent.serializedParts :+ URLEncoder.encode(part, charset)
  }
}