package ru.kolyvan.redis

trait Argument

object Argument {

 // case object MIN extends Argument
 // case object MAX extends Argument
 // case object SUM extends Argument

  case class BY(pattern: String) extends Argument { override def toString = "BY " + pattern }
  case class GET(pattern: String) extends Argument {override def toString = "GET " + pattern}
  case object ALPHA extends Argument
  case object DESC extends Argument

  case class Limit (offset: Int, count: Int) extends Argument {
    override def toString = { "LIMIT %d %d".format(offset, count) }
  }

}

