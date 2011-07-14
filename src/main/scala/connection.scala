package ru.kolyvan.redis

import java.io.{OutputStream, InputStream}
import java.net.{InetSocketAddress, Socket, SocketException}

trait Connection {
  def in : InputStream
	def out : OutputStream
  def reconnect
  def close
}

final class DefaultConnection(val host: String, val port: Int, val timeout: Int = 0) extends Connection {

	def this() = this("localhost", 6379, 0)

	private var in_  : InputStream = _
	private var out_ : OutputStream = _

  def in  = in_
  def out = out_

  connect

	private def connect() = {
		val sock = new Socket()
		sock.connect(new InetSocketAddress(host, port), timeout)
    in_ = sock.getInputStream
		out_ = sock.getOutputStream
    if (timeout > 0)
		  sock.setSoTimeout(timeout)
	}

	def reconnect {
		close
		connect
	}

  def close {
		if (out_ != null) {
		  out_.close
      out_ = null
    }
		if (in_ != null) {
      in_.close
      in_ = null
    }
	}

}

