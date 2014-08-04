package es.support

import org.scalatest.{Matchers, WordSpecLike}

class GuidSpec extends WordSpecLike with Matchers {

  "Guid" must {
    "return different on two calls to generate" in {
      assert(Guid.generate != Guid.generate)
    }
    "be equal to itself" in {
      val g = Guid.generate
      assert(g === g)
      assert(g.hashCode === g.hashCode)
    }

    "serialize to string and parse into the same" in {
      (1 to 1000) foreach { _ =>
        val g = Guid.generate
        val str = g.serializeToString
        val parsed = Guid.parseFromString(str)
        assert(parsed.isDefined)
        assert(parsed.get === g)
        assert(parsed.get.hashCode === g.hashCode)
      }
    }
    "be different in string form from other guid's string form" in {
      assert(Guid.generate.serializeToString != Guid.generate.serializeToString)
    }
    "only contain printable characters when serialized to string" in {
      val str = Guid.generate.serializeToString
      "^[a-zA-Z0-9-]{5,100}$".r.pattern.matcher(str).matches()
    }

    "serialize to bytes and parse into the same" in {
      (1 to 1000) foreach { _ =>
        val g = Guid.generate
        val bs = g.serializeToBytes
        val parsed = Guid.parseFromBytes(bs)
        assert(parsed.isDefined)
        assert(parsed.get === g)
        assert(parsed.get.hashCode == g.hashCode)
      }
    }
    "be different in byte from from other guid's bytes" in {
      assert(Guid.generate.serializeToBytes != Guid.generate.serializeToBytes)
    }
  }

}
