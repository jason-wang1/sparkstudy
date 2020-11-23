package com.qf.day06.nettytest.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

/**
  * 实现服务端
  */
class NettyServer {
  def bind(host: String, port: Int): Unit ={
    /**
      * 配置环境
      */
    // 配置服务器的线程组, 用于服务器接收客户端发过来的连接
    val group = new NioEventLoopGroup()
    // 进行用户网络读写
    val loopGroup = new NioEventLoopGroup()
    // 启动NIO服务端的辅助类
    val bootstrap = new ServerBootstrap()
    bootstrap.group(group, loopGroup).channel(classOf[NioServerSocketChannel])
    // 绑定io事件
      .childHandler(new ChannelInitializer[SocketChannel] {
      override def initChannel(c: SocketChannel) = {
        // 调用Handler，创建服务器的环境
        c.pipeline().addLast(new ServerHandler)
      }
    })


    /**
      * 绑定端口
      */
      // 绑定端口
    val channelFuture = bootstrap.bind(host, port).sync()
    // 等待服务关闭
    channelFuture.channel().closeFuture().sync()
    // 释放线程
    group.shutdownGracefully()
    loopGroup.shutdownGracefully()
  }
}

object NettyServer {
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1"
    val port = "6666".toInt
    val server = new NettyServer
    // 开始启动服务
    server.bind(host, port)
  }
}
