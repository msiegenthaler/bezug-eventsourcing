package pubsub

/** Topic. Is hierarchical, subscribing to the parent topic includes all subtopics. */
case class Topic private(path: List[String]) {
  def parent = Topic(path.dropRight(1))
  def \(child: String) = {
    require(child.nonEmpty)
    Topic(path :+ child)
  }
  override def toString = path.mkString("/")
}
object Topic {
  val root = Topic(Nil)
}
