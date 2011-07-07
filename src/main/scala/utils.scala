package ru.kolyvan.redis


object Utils {
  val zerobytes = new Array[Byte](0)

  def unexpected(x: Reply) = throw ProtocolError("unexpected reply: " + x)

  // pretty print
  def pp(bytes: Bytes): String = bytes map {
      case 10 => "\\n"
      case 13 => "\\r"
      case x if x > 31 && x < 127 => x.toChar.toString
      case x => "\\x%02x".format(x)
    } mkString


  def flatten (kvs: Seq[(String, Bytes)]): Seq[Any] = {
    // kvs.foldRight(List.empty){ case ((k, v), xs) => k :: v :: xs }
    // kvs.flatten (t => List(t._1, t._2))
    val l = List.newBuilder[Any]
    l.sizeHint(kvs.length)
    for (kv <- kvs) {
      l += kv._1
      l += kv._2
    }
    l.result
  }

  def pairify[T] (x: Seq[T]) : Seq[Pair[T,T]] = {
    val it = x.iterator
    val lb = List.newBuilder[Pair[T,T]]
    while (it.hasNext) {
      val k = it.next
      if (it.hasNext) {
        val v = it.next
        lb += (k -> v)
      } else
        sys.error("odd length list!")
    }
    lb.result
  }

  def pair[T](x: Seq[T]) =
    if (x.length == 2)
      (x(0), x(1))
    else
      sys.error("unable make pair from seq " + x)


   // http://stackoverflow.com/questions/2601611/zip-elements-with-odd-and-even-indices-in-a-list/2602071#2602071
  //def pairify[T](list: List[T]): List[(T, T)] = list match {
  //  case Nil => Nil
  //  case x :: y :: xs => (x, y) :: pairify(xs)
  //  case _ => sys.error("odd length list!")
  //}
}

