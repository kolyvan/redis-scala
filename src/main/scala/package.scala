package ru.kolyvan

package object redis {
  type Bytes = Array[Byte]

  case class ReplyError(message: String) extends RuntimeException(message)
  case class ProtocolError(message: String) extends RuntimeException(message)
  object DiscardException extends RuntimeException("discard")
  object ReconnectException extends RuntimeException("reconnect")

  // object Implicits {}
}


