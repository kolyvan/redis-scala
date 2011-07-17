package ru.kolyvan.redis.test

import _root_.ru.kolyvan.redis._
import org.scalatest.FunSuite
import scala.util.Random
import scala.concurrent.ops.spawn

//import Conv._

class MultiThreadTest extends FunSuite {


  val redis = {
    val r = Redis()
    r.select(15)
    r.flushdb
    r.sync
    // r
  }


  test("multi-thread") {

    val random = new Random()

    val minCycles = 100
    val rndCycles = 100
    val minBufsize = 4
    val rndBufsize = 60

    val threads = (1 to Runtime.getRuntime.availableProcessors() + 2) map { i =>
      new Thread(
        new Runnable {
          def run {
            Thread.sleep(1)
           // println("start thread " + i)
            val key = "testkey" + i
            val cycles = minCycles + random.nextInt(rndCycles)
            for (i <- 1 to cycles) {
              val buf = new Bytes(minBufsize + random.nextInt(rndBufsize))
              random.nextBytes(buf)
              expect(true) { redis.set(key, buf) }
              expect(true) {
                val x = redis.get(key);
                x.get sameElements  buf
              }
              expect(true) {
                redis.pipeline { r =>
                  r.set(key, buf)
                  r.get(key)
                } match {
                  case true :: Some(x: Bytes) :: Nil => x sameElements buf
                  case _ => fail("unexpeced result of pipeline")
                }
              }
              expect(true) {
                redis.multi { r =>
                  r.set(key, buf)
                  r.get(key)
                } match {
                  case true :: Some(x: Bytes) :: Nil => x sameElements buf
                  case _ => fail("unexpeced result of multi")
                }
              }
            //  println("done %s %d".format(key, i))
            }
           // println("exit thread " + i)
          }
        })
    }

    threads.foreach { t => t.start }
    threads.foreach { t => t.join }
  }

}