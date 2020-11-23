package com.qf.day06.nettytest.client

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

/**
  * 实现客户端
  */
class NettyClient {
  def bind(host: String, port: Int): Unit ={
    /**
      * 配置环境
      */
      // 线程组
    val group = new NioEventLoopGroup()
    // 客户端的辅助类
    val bootstrap = new Bootstrap
    bootstrap.group(group).channel(classOf[NioSocketChannel])
      // 绑定io事件
      .handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(c: SocketChannel) = {
        c.pipeline().addLast(new ClientHandler)
      }
    })


    /**
      * 开始绑定端口
      */
    val channelFuture = bootstrap.connect(host, port).sync()

    // 等待服务关闭
    channelFuture.channel().closeFuture().sync()
    // 释放线程
    group.shutdownGracefully()

  }

}
object NettyClient {
  def main(args: Array[String]): Unit = {
    // 定义用于请求的服务端的host和port
    val host = "127.0.0.1"
    val port = "6666".toInt

    val client = new NettyClient
    client.bind(host, port)
  }
}
