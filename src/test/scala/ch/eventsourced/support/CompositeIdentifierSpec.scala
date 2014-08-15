package ch.eventsourced.support

import org.scalatest.{Matchers, WordSpecLike}

class CompositeIdentifierSpec extends WordSpecLike with Matchers {

  "CompositeIdentifier.root" must {
    "be equal to itself" in {
      assert(CompositeIdentifier.root === CompositeIdentifier.root)
      assert(CompositeIdentifier.root.hashCode === CompositeIdentifier.root.hashCode)
    }
    "have itself as the parent" in {
      assert(CompositeIdentifier.root.parent === CompositeIdentifier.root)
    }
    "serialize to /" in {
      assert(CompositeIdentifier.root.serialize === "/")
    }
    "parse from /" in {
      assert(CompositeIdentifier.parse("/") === Some(CompositeIdentifier.root))
    }
  }

  "CompositeIdentifier" must {
    val id1 = CompositeIdentifier.root / "aggregate" / "123"
    val id2 = CompositeIdentifier.root / "and/or" / "hans peter"

    "be hierarchical" in {
      assert(id1.parent === CompositeIdentifier.root / "aggregate")
    }
    "be rooted in root" in {
      assert(id1.parent.parent === CompositeIdentifier.root)
    }
    "serialize to / seperated string" in {
      assert(id1.serialize === "/aggregate/123")
    }
    "parse from / seperated string" in {
      assert(CompositeIdentifier.parse("/aggregate/123") === Some(id1))
    }
    "url-encode special characters such as '/' in parts when serialized" in {
      assert(id2.serialize === "/and%2For/hans+peter")
    }
    "parse url-encoded parts" in {
      assert(CompositeIdentifier.parse("/and%2For/hans+peter") === Some(id2))
    }

    "parse None on invalid values" in {
      assert(CompositeIdentifier.parse("asasd") === None)
      assert(CompositeIdentifier.parse("\\asasd") === None)
    }

    "unapply from composite identifier" in {
      id1 match {
        case CompositeIdentifier("aggregate", b) => assert(b === "123")
      }
      id1 match {
        case CompositeIdentifier(a, b) =>
          assert(a === "aggregate")
          assert(b === "123")
      }
    }
    "unapply from string" in {
      id1.serialize match {
        case CompositeIdentifier("aggregate", b) => assert(b === "123")
      }
    }

    "concatenate with another CompositeIdentifier" in {
      val conc = id1 / id2
      assert(conc === CompositeIdentifier.root / "aggregate" / "123" / "and/or" / "hans peter")
    }

    "concatenate with root to be the same as before" in {
      val conc = id1 / CompositeIdentifier.root
      assert(conc === id1)
    }
  }
}