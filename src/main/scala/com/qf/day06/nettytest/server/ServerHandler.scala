package com.qf.day06.nettytest.server

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

/**
  * 用于建立服务端和客户端的连接
  */
class ServerHandler extends ChannelInboundHandlerAdapter{
  // 当客户端向服务端连接后使用此方法
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("连接成功")
    Thread.sleep(2000)
  }

  // 接收客户端发送过来的信息
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    println("接收到客户端发送过来的数据")
    // 将接收的数据进行类型转换
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val massage: String = new String(bytes, "UTF-8")

    println(massage)

    // 响应数据给客户端
    val back = "服务端发送的数据"
    val resp = Unpooled.copiedBuffer(back.getBytes("UTF-8"))
    ctx.write(resp)
  }

  // 将消息队列汇总的数据写入SocketChannel， 并发送给对方
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }
}
