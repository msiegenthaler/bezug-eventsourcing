package bezug

package object fakturierung {

  sealed trait Register
  object Register {
    object NP extends Register
  }

}
