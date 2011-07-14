package ru.kolyavan.redis.test

import _root_.ru.kolyvan.redis._
import org.scalatest.FunSuite

import Conv._

class RedisTest extends FunSuite {

  // enableLog()

  val redis = {
    val r = mkRedis()
    r.flushdb
    r
  }

  test("ping-pong") {
    expect(true) { redis.ping }
  }

  test("echo") {
    expect("tuk tuk") { redis.echo("tuk tuk") }
  }

  test("empty db") {
    expect(Nil) { redis.keys() }
  }

  test ("exists key") {
    expect(false)       { redis.exists("foo") }
    expect(true)        { redis.set("foo", B("one")) }
    expect(true)        { redis.exists("foo") }
    expect(Some("one")) { S(redis.get("foo")) }
  }

  test("nonempty db") {
    expect(1) { redis.keys().length }
  }

  test("del/rename keys") {
    expect(true)       { redis.set("moo", B("1")) }
    expect(Some("1"))  { S(redis.get("moo")) }
    expect(true)       { redis.rename("moo", "foo") }
    expect(Some("1"))  { S(redis.get("foo")) }
    expect(1)          { redis.del("foo") }
    expect(0)          { redis.del("foo") }
  }


  test("set/get") {

    expect(true)        { redis.set("foo", B("111")) }
    expect(Some("111")) { S(redis.get("foo")) }

    expect(true)        { redis.exists("foo") }
    expect(false)       { redis.exists("moo") }

    expect(true)        { redis.keys() contains "foo" }

    expect(true)        { redis.setnx("boo", B("333")) }
    expect(false)       { redis.setnx("boo", B("222")) }
    expect(Some("333")) { S(redis.get("boo")) }

    expect(Some("333")) { S(redis.getset("boo", B("444"))) }
    expect(Some("444")) { S(redis.get("boo")) }

    expect(None)        { redis.get("nope") }

    expect(true)        { redis.mset("moo" -> B("1"), "foo" -> B("2")) }

    expect(Seq(Some("1"), None, Some("2")))
      { S(redis.mget("moo", "nope", "foo")) }
  }

  test ("append/sbustr") {
    expect(3)           { redis.append("bar", B("123")) }
    expect(5)           { redis.append("bar", B("45")) }
    expect(Some("345")) { S(redis.substr("bar", 2, -1)) }
    expect(Some(""))    { S(redis.substr("bar", 1, 0)) }
    expect(Some(""))    { S(redis.substr("nope", 1, 0)) }

  }

  //if (false)
  test ("expire and ttl") {
    expect(true)  { redis.setex("temp", 1, B("1")) }
    Thread.sleep(2000)
    expect(false) { redis.exists("temp") }
  }

  test ("incr/decr") {
    expect(1)  { redis.incr("counter") }
    expect(2)  { redis.incr("counter") }
    expect(5)  { redis.incrby("counter", 3) }
    expect(4)  { redis.decr("counter") }
    expect(2)  { redis.decrby("counter", 2) }

    expect(Some("2")) { S(redis.get("counter")) }
  }


  test ("pipeline") {

    expect(Seq(true, true, Some("42"), None, Seq(Some("42"), None, Some("777")), Some("777"))) {
      S(redis.pipeline { r =>
        r.set("test1", B("42"))
        r.set("test2", B("777"))
        r.get("test1")
        r.get("unknown")
        r.mget("test1", "unknown", "test2")
        r.get("test2")
      })
    }

    expect(Some("42")) { S(redis.get("test1")) }
  }

  test("multi") {

    expect(Seq(true, true, Some("12"), None, Seq(Some("12"), None, Some("77")), Some("77"))){
      S(redis.multi { r =>
        r.set("test1", B("12"))
        r.set("test2", B("77"))
        r.get("test1")
        r.get("unknown")
        r.mget("test1", "unknown", "test2")
        r.get("test2")
      })
    }

    expect(Some("12")) { S(redis.get("test1")) }
  }

  test("discard") {

    expect(Nil) { redis.multi { r =>
        r.set("test1", B("2"))
        r.set("test2", B("3"))
        r.discard
      }}

    expect(Some("12")) { S(redis.get("test1")) }
    expect(Some("77")) { S(redis.get("test2")) }
  }

  test ("make a list") {
    expect(1) { redis.rpush("list", B("2")) }
    expect(2) { redis.rpush("list", B("3")) }
    expect(3) { redis.rpush("list", B("4")) }
    expect(4) { redis.lpush("list", B("1")) }
    expect(5) { redis.lpush("list", B("0")) }
    expect(5) { redis.llen("list") }

    expect(Seq("0", "1", "2", "3", "4")) { S(redis.lrange("list", 0, -1)) }
  }

  test ("set new value in the list") {
    expect(true)       { redis.lset("list", 2, B("22")) }
    expect(Some("22")) { S(redis.lindex("list", 2)) }
    expect(None)       { S(redis.lindex("list", 7)) }
  }

  test ("remove values from the list") {
    expect(1)  { redis.lrem("list", 0, B("22")) }
    expect(Seq("0", "1", "3", "4")) { S(redis.lrange("list", 0, -1)) }
  }

  test ("pop values from the list") {
    expect(Some("0")) { S(redis.lpop("list")) }
    expect(Some("1")) { S(redis.lpop("list")) }
    expect(Some("4")) { S(redis.rpop("list")) }
    expect(Seq("3"))  { S(redis.lrange("list", 0, -1)) }
    expect(Some("3")) { S(redis.rpop("list")) }

    expect(Nil)       { redis.lrange("list", 0, -1) }
    expect(Nil)       { redis.lrange("unknown", 0, -1) }
  }

  test ("rpoplpush") {
    expect(1) { redis.rpush("list", B("a")) }
    expect(2) { redis.rpush("list", B("b")) }
    expect(3) { redis.rpush("list", B("c")) }

    expect(Some("c")) { S(redis.rpoplpush("list", "list")) }
    expect(Seq("c", "a", "b")) { S(redis.lrange("list", 0, -1)) }
  }

  test ("blocking pop") {
    expect(Some("list" -> "c")) { S(redis.blpop(1, "unknown", "list")) }
    expect(None) { S(redis.blpop(1, "unknown")) }
  }

  test ("make a new set") {
    expect(true)  { redis.sadd("set", B("1")) }
    expect(true)  { redis.sadd("set", B("2")) }
    expect(false) { redis.sadd("set", B("2")) }
    expect(true)  { redis.sadd("set", B("3")) }

    expect(3)     { redis.scard("set") }

    expect(true)  { redis.sismember("set", B("2")) }
    expect(false) { redis.sismember("set", B("5")) }

    expect(Set("1","2","3")) { S(redis.smembers("set")).toSet }
  }

  test ("union/inter/diff") {

    expect(true) { redis.sadd("set2", B(2)) }
    expect(true) { redis.sadd("set2", B(3)) }
    expect(true) { redis.sadd("set2", B(4)) }

    expect(true) { redis.sadd("set3", B(1)) }
    expect(true) { redis.sadd("set3", B(2)) }
    expect(true) { redis.sadd("set3", B(3)) }

    expect(Set(1, 2, 3, 4)) { X[Int](redis.sunion("set2", "set3")).toSet }
    expect(Set(2, 3))       { X[Int](redis.sinter("set2", "set3")).toSet }
    expect(Set(4))          { X[Int](redis.sdiff("set2", "set3")).toSet }

  }

  test ("union/inter/diff with store") {
    expect(4) { redis.sunionstore("union", "set2", "set3") }
    expect(Set(1,2,3,4)) { X[Int](redis.smembers("union")).toSet }

    expect(2) { redis.sinterstore("inter", "set2", "set3") }
    expect(Set(2,3)) { X[Int](redis.smembers("inter")).toSet }

    expect(1) { redis.sdiffstore("diff", "set2", "set3") }
    expect(Set(4)) { X[Int](redis.smembers("diff")).toSet }
  }

  test ("make a new zset") {
    expect(true) { redis.zadd("zset", 40, B("four")) }
    expect(true) { redis.zadd("zset", 20, B("two")) }
    expect(true) { redis.zadd("zset", 10, B("one")) }
    expect(true) { redis.zadd("zset", 33, B("three")) }
    expect(false){ redis.zadd("zset", 30, B("three")) }
    expect(true) { redis.zadd("zset", 77, B("forremove")) }
    expect(true) { redis.zrem("zset", B("forremove")) }

    expect(2)       { redis.zcount("zset", 10, 20) }
    expect(4)       { redis.zcard("zset") }
    expect(Some(1)) { redis.zrank("zset", B("two")) }
    expect(None)    { redis.zrank("zset", B("unknown")) }

    expect(Some(20f))  { redis.zscore("zset", B("two")) }
    expect(None)          { redis.zscore("zset", B("unknown")) }
  }

  test ("get range") {
    expect(Seq("one", "two", "three", "four"))  { S(redis.zrange("zset", 0, -1)) }
    expect(Seq("one" -> 10f, "two" -> 20f))   { S(redis.zrange2("zset", 0, 1)) }
  }

  test ("pipeline and zset") {
    expect(Seq(
      Seq("one", "two", "three", "four"),
      Seq("one" -> 10f, "two" -> 20f),
      Some(1), Some(20f)))
      {S(redis.pipeline { r =>
       r.zrange("zset", 0, -1)
       r.zrange2("zset", 0, 1)
       r.zrank("zset", B("two"))
       r.zscore("zset", B("two"))
      })}

    expect(Seq(
      Seq("one", "two", "three", "four"),
      Seq("one" -> 10f, "two" -> 20f),
      Some(1), Some(20f)))
      {S(redis.multi { r =>
       r.zrange("zset", 0, -1)
       r.zrange2("zset", 0, 1)
       r.zrank("zset", B("two"))
       r.zscore("zset", B("two"))
      })}
  }

  test ("reverse range") {
    expect(510f) { redis.zincrby("zset", 500, B("one")) }
    expect(Seq("one", "four", "three", "two")) { S(redis.zrevrange("zset", 0, -1)) }
    expect(Seq("one" -> 510f, "four" -> 40f)) { S(redis.zrevrange2("zset", 0, 1)) }
    expect(10f) { redis.zincrby("zset", -500, B("one")) }
  }

  test ("ranbebyscore") {

    import Argument.Limit

    expect(Seq("one", "two", "three", "four"))
    { S(redis.zrangebyscore("zset")) }

    expect(Seq("two", "three", "four"))
    { S(redis.zrangebyscore("zset", 20f)) }

    expect(Seq("two"))
    { S(redis.zrangebyscore("zset", 20f, limit = Limit(0,1))) }

    expect(Seq("three", "four"))
    { S(redis.zrangebyscore("zset", 20f, false)) }

    expect(Seq("one", "two"))
    { S(redis.zrangebyscore("zset", min = 10f, max = 20f)) }

    expect(Seq("one"))
    { S(redis.zrangebyscore("zset", 10f, true, 20f, false)) }

    expect(Seq("one"->10f, "two"->20f))
    { S(redis.zrangebyscore2("zset", min = 10f, max = 20f)) }

  }

  test ("unionstore") {
    expect(true) { redis.zadd("zset2", 15, B("15")) }
    expect(true) { redis.zadd("zset2", 25, B("25")) }

    expect(6)    { redis.zunionstore("zunion", Seq("zset", "zset2")) }
    expect(Seq("one", "15", "two", "25", "three", "four")) { S(redis.zrange("zunion", 0, -1)) }

    expect(6)    { redis.zunionstore("zunion", Seq("zset", "zset2"), Seq(1, 10)) }
    expect(Seq("one", "two", "three", "four", "15", "25")) { S(redis.zrange("zunion", 0, -1)) }
  }

  test ("remove range") {
    expect(1) { redis.zremrangebyrank("zset", 0, 0) }
    expect(3) { redis.zcard("zset") }
    expect(Seq("two", "three", "four")) { S(redis.zrange("zset", 0, -1)) }

    expect(1) { redis.zremrangebyscore("zset", 35, 100) }
    expect(2) { redis.zcard("zset")}
    expect(Seq("two", "three")) { S(redis.zrange("zset")) }
  }

  test ("make a new hash") {
    expect(true)  { redis.hset("hset", "aaa", B("0")) }
    expect(true)  { redis.hset("hset", "bbb", B("0")) }
    expect(false) { redis.hset("hset", "bbb", B("1")) }
    expect(true)  { redis.hset("hset", "ccc", B("2")) }

    expect(true)  { redis.hexists("hset", "bbb") }
    expect(false) { redis.hexists("hset", "unknown") }

    expect(3) { redis.hlen("hset") }
  }

  test ("hash ops") {
    expect(Some("1")) { S(redis.hget("hset", "bbb")) }
    expect(None)      { redis.hget("hset", "unknown") }

    expect(true)      { redis.hmset("hset", "ddd" -> B("4"), "eee" -> B("5")) }

    expect(Seq(Some("0"), Some("5"), None))
    { S(redis.hmget("hset", "aaa", "eee", "unknown")) }

    expect(true)      { redis.hdel("hset", "eee") }

    expect(Seq("aaa", "bbb", "ccc", "ddd")) { redis.hkeys("hset") }
    expect(Seq("0","1","2","4")) { S(redis.hvals("hset")) }

    expect(Seq("aaa"->"0", "bbb"->"1", "ccc"->"2", "ddd"->"4"))
    { S(redis.hgetall("hset")) }
  }

  test ("hincr") {
    expect(true)      { redis.hset("hset2", "one", B("1")) }
    expect(6)         { redis.hincrby("hset2", "one", 5) }
    expect(Some("6")) { S(redis.hget("hset2", "one")) }
  }

  test ("float") {

    expect(true) { redis.set("f1", B(10.5f)) }
    expect(true) { redis.set("f2", B(-0.25f)) }
    expect(true) { redis.set("f3", B(3.14f)) }

    expect(Seq(Some(10.5f), Some(-0.25f), Some(3.14f)))
    { F(redis.mget("f1", "f2", "f3")) }

    expect(Seq(Some(10.5f), Some(-0.25f), Some(3.14f)))
    { X[Float](redis.mget("f1", "f2", "f3")) }
  }

  test ("sort") {

    import Argument._

    redis.lpush("sort", B("zz"))
    redis.lpush("sort", B("aa"))
    redis.lpush("sort", B("bb"))
    redis.lpush("sort", B("ff"))

    expect(Seq("aa", "bb", "ff", "zz")) { S(redis.sort("sort", ALPHA)) }
    expect(Seq("zz", "ff", "bb", "aa")) { S(redis.sort("sort", ALPHA, DESC)) }
    expect(Seq("bb", "ff"))             { S(redis.sort("sort", ALPHA, Limit(1,2))) }

    expect(4) { redis.sortstore("sort", "sort2", ALPHA) }
    expect(Seq("aa", "bb", "ff", "zz")) { S(redis.lrange("sort2")) }
  }

  test ("setbit/getbit") {
    expect(0) { redis.setbit("bits", 0) }
    expect(0) { redis.setbit("bits", 1) }
    expect(1) { redis.clearbit("bits", 0) }
    expect(0) { redis.getbit("bits", 0) }
    expect(1) { redis.getbit("bits", 1) }
  }

  test ("subscribe/unsubscribe") {
    expect(1) { redis.subscribe("chat") }
    expect(0) { redis.unsubscribe("chat") }
  }

  test ("publish/readmessage") {

    import scala.concurrent.ops.spawn

    expect(1) { redis.subscribe("chat") }

    spawn {
      val r = mkRedis()
      expect(1) { r.publish("chat", "knock knock") }
      r.quit
      r.close
    }

    expect(("chat", "knock knock")) { redis.readMessage() }

    expect(0) { redis.unsubscribe("chat") }

  }

  // utils

  def dump(x: => Any) {
    try {
      println(x)
    } catch {
      case e =>
        println(e)
        println(e.getStackTraceString)
    }
  }

  def enableLog() {
    Redis.logger = println _
  }

  def mkRedis() = {
    val r = Redis()
    r.select(15)
    r
  }

}