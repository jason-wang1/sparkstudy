package com.qf.day19.stringdemo;

import redis.clients.jedis.Jedis;

public class StringDemo1 {
    public static void main(String[] args) {
        // 创建一个Jedis连接
        Jedis jedis = new Jedis("node01", 6379);

        String setAck = jedis.set("s10", "1010101");
        System.out.println(setAck);

        String getAck = jedis.get("s1");
        System.out.println(getAck);

        String ping = jedis.ping();
        System.out.println(ping);


        jedis.close();
    }
}
