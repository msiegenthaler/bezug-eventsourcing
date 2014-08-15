package ch.eventsourced.support

import org.scalatest.{Matchers, WordSpecLike}

class CompositeNameSpec extends WordSpecLike with Matchers {

  "CompositeName.root" must {
    "be equal to itself" in {
      assert(CompositeName.root === CompositeName.root)
      assert(CompositeName.root.hashCode === CompositeName.root.hashCode)
    }
    "have itself as the parent" in {
      assert(CompositeName.root.parent === CompositeName.root)
    }
    "serialize to /" in {
      assert(CompositeName.root.serialize === "/")
    }
    "parse from /" in {
      assert(CompositeName.parse("/") === Some(CompositeName.root))
    }
  }

  "CompositeName" must {
    val id1 = CompositeName.root / "aggregate" / "123"
    val id2 = CompositeName.root / "and/or" / "hans peter"

    "be hierarchical" in {
      assert(id1.parent === CompositeName.root / "aggregate")
    }
    "be rooted in root" in {
      assert(id1.parent.parent === CompositeName.root)
    }
    "serialize to / seperated string" in {
      assert(id1.serialize === "/aggregate/123")
    }
    "parse from / seperated string" in {
      assert(CompositeName.parse("/aggregate/123") === Some(id1))
    }
    "url-encode special characters such as '/' in parts when serialized" in {
      assert(id2.serialize === "/and%2For/hans+peter")
    }
    "parse url-encoded parts" in {
      assert(CompositeName.parse("/and%2For/hans+peter") === Some(id2))
    }

    "parse None on invalid values" in {
      assert(CompositeName.parse("asasd") === None)
      assert(CompositeName.parse("\\asasd") === None)
    }

    "unapply from composite name" in {
      id1 match {
        case CompositeName("aggregate", b) => assert(b === "123")
      }
      id1 match {
        case CompositeName(a, b) =>
          assert(a === "aggregate")
          assert(b === "123")
      }
    }
    "unapply from string" in {
      id1.serialize match {
        case CompositeName("aggregate", b) => assert(b === "123")
      }
    }

    "concatenate with another CompositeName" in {
      val conc = id1 / id2
      assert(conc === CompositeName.root / "aggregate" / "123" / "and/or" / "hans peter")
    }

    "concatenate with root to be the same as before" in {
      val conc = id1 / CompositeName.root
      assert(conc === id1)
    }
  }
}