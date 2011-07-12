package ru.kolyvan.redis

import java.net.SocketException
import java.io.{ByteArrayOutputStream, InputStream}
import scala.annotation.tailrec

private[redis] sealed abstract class Reply {
  override def toString: String = this match {
    case NullReply    => "null"
    case IntReply(i)  => ":" + i.toString
    case StrReply(s)  => "+" + s
    case BulkReply(b) => "$" + Utils.pp(b)
    case SeqReply(xs) => "*" + xs.map(_.toString).mkString("(", " ", ")")
  }
}

private object NullReply extends Reply
private case class IntReply(x: Int) extends Reply
private case class StrReply(x: String) extends Reply
private case class BulkReply(x: Bytes) extends Reply
private case class SeqReply(x: Seq[Reply]) extends Reply


private[redis] trait ReplyProc {

  def int(x: Reply) = x match {
    case IntReply(i)    => i
    case StrReply(s)    => s.toInt
    case BulkReply(b)   => Conv.S(b) toInt
    case x              => Utils.unexpected(x)
  }

  def bool(x: Reply) = x match {
    case IntReply(1)    => true
    case StrReply("OK") => true
    case IntReply(0)    => false
    case x              => Utils.unexpected(x)
  }

  def str(x: Reply) = x match  {
    case NullReply      => ""
    case IntReply(i)    => i.toString
    case StrReply(s)    => s
    case BulkReply(b)   => Conv.S(b)
    case x              => Utils.unexpected(x)
  }

  def opt(x: Reply) = x match {
    case NullReply      => None
    case BulkReply(b)   => Some(b)
    case x              => Utils.unexpected(x)
  }

  def opts(x: Reply): Seq[Option[Bytes]] = x match {
    case NullReply      => Nil
    case SeqReply(s)    => s.map {
      case NullReply     => None
      case BulkReply(b)  => Some(b)
      case x             => Utils.unexpected(x)
      }
    case _ => Utils.unexpected(x)
  }

  def seq(x: Reply): Seq[Bytes] = opts(x) flatten

}


private object ReplyReader {

  private def readline(inp: InputStream): (Char, String) = {

    val bos = new ByteArrayOutputStream

    @tailrec
    def readbyte() {
      inp.read match {
        case -1 => throw new SocketException("Connection reset")
        case 13 => // \r
          inp.read match {
            case -1 => throw new SocketException("Connection reset")
            case 10 => // stop \n
            case x  =>
              bos.write(13.toByte)
              bos.write(x.toByte)
              readbyte()
          }
        case x =>
          bos.write(x.toByte)
          readbyte()
      }
    }

    inp.read match {
      case -1 =>
        throw new SocketException("Connection reset")

      case x =>
        readbyte()
        x.toChar -> new String(bos.toByteArray, "UTF-8")
    }
  }

  private def readMulti(inp: InputStream, line: String): Reply =
    (try { line.toInt }
     catch {
        case e: NumberFormatException => throw ProtocolError("got " + line + " as multi reply")
     }) match {
      case -1 => NullReply
      case n: Int =>
        SeqReply(List.fill(n)(read(inp)))
    }


  private def readBulk(inp: InputStream, line: String): Reply =
    line.toInt match {
      case -1 =>
        NullReply
      case n: Int =>
        val data = new Array[Byte](n)
        inp.read(data, 0, n)
        inp.skip(2) // skip CRLF
        BulkReply(data)
      case _ =>
        throw ProtocolError("got " + line + " as bulk reply")
    }

  def read(inp: InputStream): Reply = {
    val (chr, line) = readline(inp)
    chr match {
      case '-' => throw ReplyError(line)
      case '+' => StrReply(line)
      case ':' => IntReply(line.toInt)
      case '$' => readBulk(inp, line)
      case '*' => readMulti(inp, line)
      case _   => throw ProtocolError("got " + chr + " as initial reply bits")
    }
  }
}
