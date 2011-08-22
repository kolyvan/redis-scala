package ru.kolyvan.redis


object Conv {     // from/to bytes

  // string

  def B(x: String):Bytes = x.getBytes("UTF-8")
  implicit def S(x: Bytes): String = new String(x, "UTF-8")

  def S(x: Seq[_]): Seq[_] = X(x)(S)
  def S(x: Pair[_,_]): Pair[_,_] = X(x)(S)
  def S(x: Option[_]): Option[_] = X(x)(S)

  // int

  def B(x: Int):Bytes = Array[Byte](
    ((x     )&0xFF).asInstanceOf[Byte],
    ((x>>  8)&0xFF).asInstanceOf[Byte],
    ((x>> 16)&0xFF).asInstanceOf[Byte],
    ((x>> 24)&0xFF).asInstanceOf[Byte])


  implicit def I(x: Bytes): Int = {
    (x(0).asInstanceOf[Int] & 0xFF)       |
    (x(1).asInstanceOf[Int] & 0xFF) << 8  |
    (x(2).asInstanceOf[Int] & 0xFF) << 16 |
    (x(3).asInstanceOf[Int] & 0xFF) << 24
  }

  def I(x: Seq[_]): Seq[_] = X(x)(I)
  def I(x: Pair[_,_]): Pair[_,_] = X(x)(I)
  def I(x: Option[_]): Option[_] = X(x)(I)

  // long

  def B(x: Long): Bytes = Array[Byte](
    ((x     )&0xFF).asInstanceOf[Byte],
    ((x>>  8)&0xFF).asInstanceOf[Byte],
    ((x>> 16)&0xFF).asInstanceOf[Byte],
    ((x>> 24)&0xFF).asInstanceOf[Byte],
    ((x>> 32)&0xFF).asInstanceOf[Byte],
    ((x>> 40)&0xFF).asInstanceOf[Byte],
    ((x>> 48)&0xFF).asInstanceOf[Byte],
    ((x>> 56)&0xFF).asInstanceOf[Byte])


  implicit def L(x: Bytes): Long = {
    (x(0).asInstanceOf[Long] & 0xFF)       |
    (x(1).asInstanceOf[Long] & 0xFF) <<  8 |
    (x(2).asInstanceOf[Long] & 0xFF) << 16 |
    (x(3).asInstanceOf[Long] & 0xFF) << 24 |
    (x(4).asInstanceOf[Long] & 0xFF) << 32 |
    (x(5).asInstanceOf[Long] & 0xFF) << 40 |
    (x(6).asInstanceOf[Long] & 0xFF) << 48 |
    (x(7).asInstanceOf[Long] & 0xFF) << 56
  }

  def L(x: Seq[_]): Seq[_] = X(x)(L)
  def L(x: Pair[_,_]): Pair[_,_] = X(x)(L)
  def L(x: Option[_]): Option[_] = X(x)(L)

  // float

  def B(x: Float):Bytes = B(java.lang.Float.floatToRawIntBits(x))
  implicit def F(x: Bytes): Float = java.lang.Float.intBitsToFloat(I(x))

  def F(x: Seq[_]): Seq[_] = X(x)(F)
  def F(x: Pair[_,_]): Pair[_,_] = X(x)(F)
  def F(x: Option[_]): Option[_] = X(x)(F)

  // double

  def B(x: Double):Bytes = B(java.lang.Double.doubleToRawLongBits(x))
  implicit def D(x: Bytes): Double = java.lang.Double.longBitsToDouble(L(x))

  def D(x: Seq[_]): Seq[_] = X(x)(D)
  def D(x: Pair[_,_]): Pair[_,_] = X(x)(D)
  def D(x: Option[_]): Option[_] = X(x)(D)

  // boolean

  def B(x: Boolean):Bytes = if (x) Array(1.toByte) else Array(0.toByte)
  implicit def T(x: Bytes): Boolean = x(0) != 0

  def T(x: Seq[_]): Seq[_] = X(x)(T)
  def T(x: Pair[_,_]): Pair[_,_] = X(x)(T)
  def T(x: Option[_]): Option[_] = X(x)(T)

  // any

  def X[T](x: Bytes)(implicit fromBytes: Bytes => T): T = fromBytes(x)

  def X[T](x: Seq[_])(implicit fromBytes: Bytes => T): Seq[_] = x map {
    case x: Bytes       => fromBytes(x)
    case x: Option[_]   => X(x)(fromBytes)
    case x: Pair[_,_]   => X(x)(fromBytes)
    case x: Seq[_]      => X(x)(fromBytes)
    case x              => x
  }

  def X[T](x: Pair[_,_])(implicit fromBytes: Bytes => T): Pair[_,_] =
    x match {
      case (a: Bytes, b: Bytes) => fromBytes(a) -> fromBytes(b)
      case (a, b: Bytes) => a -> fromBytes(b)
      case (a: Bytes, b) => fromBytes(a) -> b
      case _ => x
    }

  def X[T](x: Option[_])(implicit fromBytes: Bytes => T): Option[_] = x match {
    case None    => None
    case Some(x) =>
      x match {
        case x: Bytes     => Some(fromBytes(x))
        case x: Pair[_,_] => Some(X(x)(fromBytes))
        case _            => Some(x)
      }
  }
}