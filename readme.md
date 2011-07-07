# redis-scala

A Scala client library for the [Redis](http://redis.io/) key-value store.

Tested with Redis version 2.2.9, Scala version 2.9.0.1 and Xsbt version 0.10


## Requirements

- [xsbt](https://github.com/harrah/xsbt/wiki/Setup)

## Usage

build redis-scala library

    $ cd redis-scala
    $ sbt
    > update
    > test
    > console

let's play with redis

    scala> import Conv._
    scala> val r = Redis()
    scala> r.set("key", B(42))
    scala> I(r.get("key"))


## Some code and ideas was taken from following projects, thank you.

- [Alejandro Crosa] (https://github.com/acrosa/scala-redis)
- [Ezra Zygmuntowicz] (https://github.com/ezmobius/redis-rb)



