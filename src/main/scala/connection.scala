package ru.kolyvan.redis

import java.io.{OutputStream, InputStream}
import java.net.{InetSocketAddress, Socket, SocketException}

trait Connection {
  def close
  def sendCmd(bytes: Bytes)(f: InputStream => Reply): Reply

  // def reconnect
  // def readline
}

final class DefaultConnection(val host: String, val port: Int, val timeout: Int = 0) extends Connection {

	def this() = this("localhost", 6379, 0)

	private var inp: InputStream = _
	private var out: OutputStream = _

  connect

	private def connect() = {
		val sock = new Socket()
		sock.connect(new InetSocketAddress(host, port), timeout)
    inp = sock.getInputStream
		out = sock.getOutputStream
    if (timeout > 0)
		  sock.setSoTimeout(timeout)
	}

	private def disconnect {
		out.close
		inp.close
	}

	private def reconnect {
		disconnect
		connect
	}

  def close {
		if (inp != null) {
				try { disconnect }
				inp = null
        out = null
    }
	}

  def sendCmd(payload: Bytes)(f: InputStream => Reply): Reply =  {

    def op = {
      Redis.log(">> " + Utils.pp(payload))
      out.write(payload)
      val r = f(inp)
      Redis.log("<< " + r)
      r
    }

	  try {
			op
		} catch {
			case e: SocketException if e.getMessage == "Connection reset"  =>
				reconnect
	  		op
		}
	}


}

