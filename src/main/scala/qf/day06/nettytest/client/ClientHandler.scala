package com.qf.day06.nettytest.client

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

class ClientHandler extends ChannelInboundHandlerAdapter{

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("发送消息请求")
    val content = "Hi~ Server"
    ctx.writeAndFlush(Unpooled.copiedBuffer(content.getBytes("UTF-8")))
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    println("接收到服务端发送过来的消息")
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message = new String(bytes, "UTF-8")

    println(message)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

}
