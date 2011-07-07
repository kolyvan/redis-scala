package ru.kolyvan.redis

import java.io.{InputStream}


abstract class RedisOp extends Request with ReplyProc {

  protected def call[T](payload: Bytes)(rp: Reply => T): T

  def discard = sys.error("must be called only inside multi block")

  def ping = call(plain('ping)){ r =>
    str(r) match {
      case "PONG" => true
      case _      => false
    }
  }

  def echo(message: String) = call(unified('echo, message))(str)

  // general ops

  def exists(key: String): Boolean =
    call(plain('exists, key))(bool)

  def del(keys: String*): Int =
    call(unified('del, keys: _*))(int)

  def keys(pattern: String = "*"): Seq[String] =
    call(plain('keys, pattern)) { x => seq(x) map (Conv.S) }

  def valueType(key: String): String =
    call(plain('type, key))(str)

  def randomkey: String =
    call(plain('randomkey))(str)

  def rename(oldname: String, newname: String): Boolean =
    call(plain('rename, oldname, newname))(bool)

  def renamenx(oldname: String, newname: String): Boolean =
    call(plain('renamenx, oldname, newname))(bool)

  def move(key: String, db: Int): Boolean =
    call(plain('move, key, db))(bool)

  def expire(key: String, seconds: Int): Boolean =
    call(plain('expire, key, seconds))(bool)

  def expireat(key: String, unixtime: Int): Boolean =
    call(plain('expireat, key, unixtime))(bool)

  def persist(key: String):Boolean =    // 2.1.2
    call(plain('persist, key))(bool)

  def ttl(key: String): Int =
    call(plain('ttl, key))(int)

  def select(index: Int): Boolean =
    call(unified('select, index))(bool)

  def flushdb(): Boolean =
    call(plain('flushdb))(bool)


  // string ops

  def set(key: String, value: Bytes): Boolean =
    call(unified('set, key, value))(bool)

  def get(key: String): Option[Bytes] =
    call(plain('get, key))(opt)

  def setnx(key: String, value: Bytes): Boolean =
    call(unified('setnx, key, value))(bool)

  def setex(key: String, seconds: Int, value: Bytes): Boolean =
    call(unified('setex, key, seconds, value))(bool)

  def getset(key: String, value: Bytes): Option[Bytes] =
    call(unified('getset, key, value))(opt)

  def mget(keys: String*): Seq[Option[Bytes]] =
    call(unified('mget, keys: _*))(opts)

  def mset(kvs: (String, Bytes)*): Boolean =
    call(unified('mset, Utils.flatten(kvs): _*))(bool)

  def msetnx(kvs: (String, Bytes)*): Boolean =
    call(unified('msetnx, Utils.flatten(kvs): _*))(bool)

  def append(key: String, value: Bytes): Int =
    call(unified('append, key, value))(int)

  def substr(key: String, start: Int, end: Int): Option[Bytes] =
    call(plain('substr, key, start, end))(opt)


  // all inc\dec stored as String

  def incr(key: String): Int =
    call(plain('incr, key))(int)

  def decr(key: String): Int =
    call(plain('decr, key))(int)

  def incrby(key: String, value: Int): Int =
    call(plain('incrby, key, value))(int)

  def decrby(key: String, value: Int): Int =
    call(plain('decrby, key, value))(int)

  // list ops

  def rpush(key: String, value: Bytes): Int =
    call(unified('rpush, key, value))(int)

  def lpush(key: String, value: Bytes): Int =
    call(unified('lpush, key, value))(int)

  def llen(key: String): Int =
    call(plain('llen, key))(int)

  def lrange(key: String, start: Int = 0, end: Int = -1): Seq[Bytes] =
    call(plain('lrange, key, start, end))(seq)

  def ltrim(key: String, start: Int, end: Int): Boolean  =
    call(plain('ltrim, key, start, end))(bool)

  def lindex(key: String, index: Int): Option[Bytes] =
    call(plain('lindex, key, index))(opt)

  def lset(key: String, index: Int, value: Bytes): Boolean =
    call(unified('lset, key, index, value))(bool)

  def lrem(key: String, index: Int, value: Bytes): Int =
    call(unified('lrem, key, index, value))(int)

  def lpop(key: String): Option[Bytes] =
    call(plain('lpop, key))(opt)

  def rpop(key: String): Option[Bytes] =
    (call(plain('rpop, key))(opt))

  def blpop(timeout: Int, keys: String*): Option[(String, Bytes)] =
    call(plain('blpop, keys.toList ::: timeout :: Nil: _*)) { x =>
      seq(x) match {
        case Nil => None
        case k::v::Nil => Some(Conv.S(k) -> v)
        case _ => Utils.unexpected(x)
      }
    }

  def brpop(timeout: Int, keys: String*): Option[(String, Bytes)] =
    call(plain('brpop, keys.toList ::: timeout :: Nil: _*)){ x =>
      seq(x) match {
        case Nil => None
        case k::v::Nil => Some(Conv.S(k) -> v)
        case _ => Utils.unexpected(x)
      }
    }

  def rpoplpush(srckey: String, dstkey: String): Option[Bytes] =
    call(plain('rpoplpush, srckey, dstkey))(opt)

   // set ops

  def sadd(key: String, value: Bytes): Boolean =
    call(unified('sadd, key, value))(bool)

  def srem(key: String, value: Bytes): Boolean =
    call(unified('srem, key, value))(bool)

  def spop(key: String): Option[Bytes] =
    call(plain('spop, key))(opt)

  def scard(key: String): Int =
    call(plain('scard, key))(int)

  def sismember(key: String, value: Bytes): Boolean =
    call(unified('sismember, key, value))(bool)

  def sinter(keys: String*): Seq[Bytes] =
    call(unified('sinter, keys: _*))(seq)

  def sinterstore(dstkey: String, keys: String*): Int =
    call(plain('sinterstore, dstkey :: keys.toList: _*))(int)

  def sunion(keys: String*): Seq[Bytes] =
    call(unified('sunion, keys: _*))(seq)

  def sunionstore(dstkey: String, keys: String*): Int =
    call(plain('sunionstore, dstkey :: keys.toList: _*))(int)

  def sdiff(keys: String*): Seq[Bytes] =
    call(unified('sdiff, keys: _*))(seq)

  def sdiffstore(dstkey: String, keys: String*): Int =
    call(plain('sdiffstore, dstkey :: keys.toList: _*))(int)

  def smembers(key: String): Seq[Bytes] =
    call(plain('smembers, key))(seq)

  def srandmember(key: String): Option[Bytes] =
    call(plain('srandmember, key))(opt)

  def smove(srckey: String, dstkey: String, value: Bytes): Boolean =
    call(unified('smove, srckey, dstkey, value))(bool)


  // zset ops

  def zadd(key: String, score:Int, value: Bytes): Boolean =
    call(unified('zadd, key, score, value))(bool)

  def zrem(key: String, value: Bytes): Boolean =
    call(unified('zrem, key, value))(bool)

  def zincrby(key: String, increment:Int, value: Bytes): Float =
    call(unified('zincrby, key, increment, value)){ x => str(x) toFloat }

  def zrank(key: String, value: Bytes): Option[Int] =    // 2.1.2 int
    call(unified('zrank, key, value)) { // empty orElse int andThen { }
      case NullReply      => None
      case IntReply(i)    => Some(i)
      case x              => Utils.unexpected(x)
    }

  def zrange(key: String, start: Int = 0, end: Int = -1): Seq[Bytes] =
    call(plain('zrange, key, start, end))(seq)

  def zrevrange(key: String, start: Int = 0, end: Int = -1): Seq[Bytes] =
    call(plain('zrevrange, key, start, end))(seq)

  def zcount(key: String, min: Int, max: Int): Int =
    call(plain('zcount, key, min, max))(int)

  def zcard(key: String): Int =
    call(plain('zcard, key))(int)

  def zrange2(key: String, start: Int = 0, end: Int = -1): Seq[(Bytes, Float)] =
    call(plain('zrange, key, start, end, "WITHSCORES")) { x =>
      Utils.pairify(seq(x)) map { case (k, v) => (k, Conv.S(v) toFloat) }
    }

  def zrevrange2(key: String, start: Int = 0, end: Int = -1): Seq[(Bytes, Float)] =
    call(plain('zrevrange, key, start, end, "WITHSCORES")) { x =>
      Utils.pairify(seq(x)) map { case (k, v) => (k, Conv.S(v) toFloat) }
    }

  def zscore(key: String, value: Bytes): Option[Float] =
    call(unified('zscore, key, value)) { x => opt(x) match {
        case None => None
        case Some(b) => Some(Conv.S(b) toFloat)
      }
    }

  def zrangebyscore(key: String,
                    min: Double = Double.NegativeInfinity,
                    minInclusive: Boolean = true,
                    max: Double = Double.PositiveInfinity,
                    maxInclusive: Boolean = true,
                    limit: Argument.Limit = null): Seq[Bytes] = {
    call(plain('zrangebyscore,
      buildZrangebyscoreArgs(key, min, minInclusive, max, maxInclusive, limit, false): _*))(seq)
  }

  def zrangebyscore2(key: String,
                     min: Double = Double.NegativeInfinity,
                     minInclusive: Boolean = true,
                     max: Double = Double.PositiveInfinity,
                     maxInclusive: Boolean = true,
                     limit: Argument.Limit = null): Seq[(Bytes, Float)] = {
    call(plain('zrangebyscore,
      buildZrangebyscoreArgs(key, min, minInclusive, max, maxInclusive, limit, true) : _*)){ x =>
        Utils.pairify(seq(x)) map { case (k, v) => (k, Conv.S(v) toFloat) }
    }
  }

  def zremrangebyrank(key: String, start: Int, end: Int): Int =
    call(plain('zremrangebyrank, key, start, end))(int)

  def zremrangebyscore(key: String, min: Int, max: Int): Int =
    call(plain('zremrangebyscore, key, min, max))(int)

  def zunionstore(dstkey: String, keys: Seq[String], weights: Seq[Int]=Nil, aggregate:String=null): Int =
    call(plain('zunionstore, buildZstoreArgs(dstkey,keys,weights,aggregate) :_*))(int)

  def zinterstore(dstkey: String, keys: Seq[String], weights: Seq[Int]=Nil, aggregate:String=null): Int =
    call(plain('zinterstore, buildZstoreArgs(dstkey,keys,weights,aggregate) :_*))(int)

  // hash ops

  def hset(key: String, field:String, value: Bytes): Boolean =
    call(unified('hset, key, field, value))(bool)

  def hget(key: String, field:String): Option[Bytes] =
    call(unified('hget, key, field))(opt)

  def hmset(key: String, kvs:(String, Bytes)*): Boolean =
    call(unified('hmset, key +: Utils.flatten(kvs): _*))(bool)

  def hmget(key: String, fields:String*): Seq[Option[Bytes]] =
    call(unified('hmget, key +: fields : _*))(opts)

  def hincrby(key: String, field:String, value: Int): Int =
    call(unified('hincrby, key, field, value))(int)

  def hexists(key: String, field:String): Boolean =
    call(unified('hexists, key, field))(bool)

  def hdel(key: String, field:String): Boolean =
    call(unified('hdel, key, field))(bool)

  def hlen(key: String): Int =
    call(unified('hlen, key))(int)

  def hkeys(key: String): Seq[String] =
    call(unified('hkeys, key)) { seq(_) map (Conv.S) }

  def hvals(key: String): Seq[Bytes] =
    call(unified('hvals, key))(seq)

  def hgetall(key: String): Seq[(String, Bytes)] =
    call(unified('hgetall, key)) { x =>
       Utils.pairify(seq(x)) map { case (k, v) => (Conv.S(k), v) }
    }

  // sort ops

  def sort(key: String, args: Argument*): Seq[String] =
    call(plain('sort, key +: args : _*)){ seq(_) map (Conv.S)}

  def sortstore(key: String, store: String, args: Argument*): Int =
    call(plain('sort, key :: "STORE" :: store :: args.toList : _*))(int)


  // bits op 2.1.8

  def setbit(key: String, offset: Int): Int =
    call(unified('setbit, key, offset, 1))(int)

  def clearbit(key: String, offset: Int): Int =
    call(unified('setbit, key, offset, 0))(int)

  def getbit(key: String, offset: Int): Int =
    call(unified('getbit, key, offset))(int)

}

class Redis(conn: Connection) extends RedisOp {

  private abstract class DelayedRedis extends RedisOp {
    type ReplyFunc = Function[Reply, Any]
    var funcs: List[ReplyFunc] = Nil
    def addreply(f: Reply => Any)  { funcs = f :: funcs }
  }

  private class PipelineRedis extends DelayedRedis {

    def call[T](payload: Bytes)(f: Reply => T): T = {
      conn.sendCmd(payload){ _ => NullReply }
      addreply(f)
      null.asInstanceOf[T]
    }

    def exec(inp: InputStream): Seq[Any] = {
      funcs.reverse.map { f => f(ReplyReader.read(inp)) }
    }
  }

  private class MultiRedis extends DelayedRedis {

    def call[T](payload: Bytes)(f: Reply => T): T = {
      conn.sendCmd(payload){ inp =>
        ReplyReader.read(inp) match {
          case StrReply(s) if s == "QUEUED" => NullReply
          case x => Utils.unexpected(x)
        }
      }
      addreply(f)
      null.asInstanceOf[T]
    }

    override def discard = throw DiscardException

    def exec(x: Reply): Seq[Any] = {
      funcs = funcs.reverse
      x match {
        case NullReply => Nil
        case SeqReply(s) =>
          assert(funcs.length == s.length)
          s.map { x =>
            val f = funcs.head
            funcs = funcs.tail
            f(x)
          }
        case x => Utils.unexpected(x)
      }
    }

  }

  def close = conn.close

  protected def call[T](payload: Bytes)(rp: Reply => T): T =
    rp(conn.sendCmd(payload){ inp => ReplyReader.read(inp) })

  def pipeline(f: RedisOp => Unit): Seq[Any] = {
    val p = new PipelineRedis
    f (p)
    var r: Seq[Any] = Nil
    conn.sendCmd(Utils.zerobytes) { inp =>
      r = p.exec(inp) // read replies
      NullReply
    }
    r
  }

   // transaction multi/discard/exec
  def multi(f: RedisOp => Unit): Seq[Any] = {

    if (!call(unified('multi))(bool))
      throw ProtocolError("got false as multi-command reply")

    val m = new MultiRedis

    try {
      f (m)
      call(unified('exec))(m.exec)
    }
    catch {
      case DiscardException =>
        call(unified('discard))(bool)
        Nil
      case e =>
        call(unified('discard))(bool)
        throw e   // rethrow
    }
  }

  def watch(keys: String*): Boolean  = call(unified('watch, keys: _*))(bool)
  def unwatch(keys: String*): Boolean  = call(unified('unwatch, keys: _*))(bool)

  // publish/subscribe

  def subscribe(channel: String): Int =
    call(unified('subscribe, channel)) {
      case SeqReply(s) if s.length == 3 =>  // subscribe channel total
        assert(str(s(0)) == "subscribe")
        assert(str(s(1)) == channel)
        int(s(2))
      case x  => Utils.unexpected(x)
    }

  def unsubscribe(channel: String) =
    call(unified('unsubscribe, channel)) {
      case SeqReply(s) if s.length == 3 => // unsubscribe channel total
        assert(str(s(0)) == "unsubscribe")
        assert(str(s(1)) == channel)
        int(s(2))
      case x  => Utils.unexpected(x)
    }


  def readMessage(): (String, String) = { // channel message
    call(Utils.zerobytes) {
      case SeqReply(s) if s.length == 3 =>
        assert(str(s(0)) == "message")
        val channel = str(s(1))
        val message = str(s(2))
        (channel, message)
      case x  => Utils.unexpected(x)
    }
  }

  def publish(channel: String, message: String): Int =
     call(unified('publish, channel, message))(int)


   // misc

  def auth(password: String): Boolean = call(plain('auth, password))(bool)
  def dbsize() = call(plain('dbsize))(int)
  def save() = call(plain('save))(bool)
  def bgsave() = call(plain('bgsave))(bool)
  def lastsave() = call(plain('lastsave))(int)
  def shutdown(): Unit = call(plain('shutdown)) { _ => () }
  def quit(): Unit = call(plain('quit)) { _ => () }

  def rawinfo() = call(plain('info))(str)
  def info() = {
    val s: Seq[(String, String)] = call(plain('info)) {
      str(_) split (CRLF) map { x => Utils.pair(x.split(':')) }
    }
    Map(s : _*)
  }

  // object monitor config

}

object Redis {
  def apply(host: String = "localhost", port: Int =  6379, timeout: Int = 0) =
    new Redis(new DefaultConnection(host, port, timeout))

  var logger: Function1[String, Unit] = null
  def log(msg: => String): Unit = if (logger != null) logger(msg)
}

