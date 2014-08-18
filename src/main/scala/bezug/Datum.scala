package bezug

case class DatumBereich(von: Datum, bis: Datum)

case class Zeitpunkt private(msSince1970: Long)
object Zeitpunkt {
  def now: Zeitpunkt = Zeitpunkt(System.currentTimeMillis)
}

case class Datum(day: Short, monat: Monat, jahr: Jahr) {
  require(day >= 1)
  require(day <= 31)
  override def toString = s"$day.${monat.index}.${jahr.value}"
}

case class Jahr(value: Short) {
  require(value >= 1700)
  require(value < 3000)
  override def toString = value.toString
}

sealed trait Monat {
  def index: Int
}
object Monat {
  def apply(index: Int): Monat =
    all.find(_.index == index).getOrElse(throw new IllegalArgumentException(s"$index is not a valid month"))

  val all = Januar :: Februar :: März :: April :: Mai :: Juni :: Juli ::
    August :: September :: Oktober :: November :: Dezember :: Nil

  object Januar extends Monat {
    def index = 1
  }
  object Februar extends Monat {
    def index = 2
  }
  object März extends Monat {
    def index = 3
  }
  object April extends Monat {
    def index = 4
  }
  object Mai extends Monat {
    def index = 5
  }
  object Juni extends Monat {
    def index = 6
  }
  object Juli extends Monat {
    def index = 7
  }
  object August extends Monat {
    def index = 8
  }
  object September extends Monat {
    def index = 9
  }
  object Oktober extends Monat {
    def index = 10
  }
  object November extends Monat {
    def index = 11
  }
  object Dezember extends Monat {
    def index = 12
  }
}
