package ru.kolyvan.redis

import java.io.{ByteArrayOutputStream}

private[redis] trait Request { // self: Redis =>

  protected val BULK = "$"
  protected val CRLF = "\r\n"

  protected val BULK_BYTES = BULK.getBytes
  protected val CRLF_BYTES = CRLF.getBytes

  protected def buildZrangebyscoreArgs(key: String,
                                       min: Double,
                                       minInclusive: Boolean,
                                       max: Double,
                                       maxInclusive: Boolean,
                                       limit: Argument.Limit,
                                       withScores: Boolean): Seq[Any] = {

    def pp (value: Double, inclusive: Boolean) = {
      (if (inclusive) "" else "(") +
      (if (value.isPosInfinity) "+inf" else if (value.isNegInfinity) "-inf" else value.toString)
    }

    val l = if (withScores) List("WITHSCORES") else Nil
    key :: pp(min, minInclusive) :: pp(max, maxInclusive) :: (if (limit != null) limit :: l else l)
  }

  protected def buildZstoreArgs(dstkey: String,
                                keys: Seq[String],
                                weights: Seq[Int],
                                aggregate: String): Seq[Any] = {
    // dstkey :: keys.length :: keys ::: weights ::: aggregate :: Nil
    val lb = List.newBuilder[Any]
    lb += dstkey
    lb += keys.length
    lb ++= keys
    if (weights != Nil) {
      lb += "WEIGHTS"
      lb ++= weights
    }
    if (aggregate != null && aggregate.nonEmpty) {
      lb += "AGGREGATE"
      lb += aggregate
    }
    lb.result
  }

  protected def plain(cmd: Symbol, args: Any*): Bytes =  {
 // println("plain: " + cmd.name + " " + args.mkString(" "))
    Conv.B(cmd.name + " " + args.mkString(" ") + CRLF)
  }

  protected def unified(cmd: Symbol, args: Any*): Bytes = {

    val bos = new ByteArrayOutputStream

    def write(b: Bytes) {
      bos.write(BULK_BYTES)
      bos.write(b.length.toString.getBytes)
      bos.write(CRLF_BYTES)
      bos.write(b)
      bos.write(CRLF_BYTES)
    }

    bos.write(Conv.B("*%d".format(args.length + 1)))
    bos.write(CRLF_BYTES)

    write(Conv.B(cmd.name))

    for (x <- args)
      write(x match {
        case b: Bytes => b
        case s => Conv.B(s.toString)
      })

    bos.toByteArray
  }
}
