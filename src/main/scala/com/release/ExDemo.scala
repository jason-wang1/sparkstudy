package com.release

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * rdd数据写入数据库
  */
object ExDemo {
  def main(args: Array[String]): Unit = {
    /*
    IP地址归属地信息

    1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    1.0.8.0|1.0.15.255|16779264|16781311|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
    1.0.32.0|1.0.63.255|16785408|16793599|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
    1.1.0.0|1.1.0.255|16842752|16843007|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    1.1.2.0|1.1.7.255|16843264|16844799|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    1.1.8.0|1.1.63.255|16844800|16859135|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
    1.2.0.0|1.2.1.255|16908288|16908799|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302

用户访问日志信息

    20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
    20090121000132124542000|117.101.215.133|www.jiayuan.com|/19245971|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; TencentTraveler 4.0)|http://photo.jiayuan.com/index.php?uidhash=d1c3b69e9b8355a5204474c749fb76ef|__tkist=0; myloc=50%7C5008; myage=2009; PROFILE=14469674%3A%E8%8B%A6%E6%B6%A9%E5%92%96%E5%95%A1%3Am%3Aphotos2.love21cn.com%2F45%2F1b%2F388111afac8195cc5d91ea286cdd%3A1%3A%3Ahttp%3A%2F%2Fimages.love21cn.com%2Fw4%2Fglobal%2Fi%2Fhykj_m.jpg; last_login_time=1232454068; SESSION_HASH=8176b100a84c9a095315f916d7fcbcf10021e3af; RAW_HASH=008a1bc48ff9ebafa3d5b4815edd04e9e7978050; COMMON_HASH=45388111afac8195cc5d91ea286cdd1b; pop_1232093956=1232468896968; pop_time=1232466715734; pop_1232245908=1232469069390; pop_1219903726=1232477601937; LOVESESSID=98b54794575bf547ea4b55e07efa2e9e; main_search:14469674=%7C%7C%7C00; registeruid=14469674; REG_URL_COOKIE=http%3A%2F%2Fphoto.jiayuan.com%2Fshowphoto.php%3Fuid_hash%3D0319bc5e33ba35755c30a9d88aaf46dc%26total%3D6%26p%3D5; click_count=0%2C3363619

     */

    //创建sc
    val conf = new SparkConf().setAppName("exdemo").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //处理字典数据
    val ipDict:RDD[String] = sc.textFile(args(1))
    val ipInfoRDD:RDD[(String,String,String)] = ipDict.map(line=>{
      val arr = line.split("|")
      val startIP = arr(2)
      val endIP = arr(3)
      val province = arr(6)
      (startIP,endIP,province)
    })
    //RDD数据广播之前要收集到driver端
    val arrIP = ipInfoRDD.collect()
    val broadcastVar = sc.broadcast(arrIP)
    //读取日志数据
    val lines: RDD[String] = sc.textFile(args(0))
    val provinceRDD = lines.map(line=>{
     val arr =  line.split("|")
      val ip = arr(1)
      //ip =>省份信息
      //把IP信息转化成long数值
      val ipLongNum = ipToLong(ip)
      val ipDict = broadcastVar.value
      //在字典数据里查找对应的省份信息

      val province = BinarySearch(ipDict,ipLongNum)
      (province,1)
    })

    val res:RDD[(String,Int)] = provinceRDD.reduceByKey(_+_)
    res.foreachPartition(it=>dataToMysql(it))
    //结果存入MySQL
   // dataToMysql(res)
    sc.stop()
  }
//把IP信息转化成long数值
  def ipToLong(ip:String)={
    val ipFragmengt: Array[String] = ip.split("[.]")
    var ipLongNum = 0l
    //通过移位操作，转化成Long类型的数值
    for(elem <-ipFragmengt)
      {
        ipLongNum = elem.toLong | ipLongNum << 8L
      }
    ipLongNum
  }

  //long数值ip在字典数据里进行匹配，返回省份信息
  def BinarySearch(arr:Array[(String,String,String)],ip:Long):String={
    var low = 0
    var high = arr.length
    while (low <= high){
      val mid = (low+high)/2
      if((ip >= arr(mid)._1.toLong)&& ip <=arr(mid)._2.toLong)
        return arr(mid)._3

      if(ip < arr(mid)._1.toLong)
        high = mid-1
      else
        low = mid+1
    }
    "undifined"
  }

  def dataToMysql(iterator: Iterator[(String, Int)])={
    var conn:Connection = null
    var ps:PreparedStatement = null
    val sql = "insert into res_info(province,counts,datainfo) values(?,?,?)"
    try{
      conn = DriverManager.getConnection("urlinfo","root","123456")
      iterator.foreach(item=>{
       ps =  conn.prepareStatement(sql)
        ps.setString(1,item._1)
        ps.setInt(2,item._2)
        ps.setDate(3,new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    }catch {
      case e:Exception =>println(e.printStackTrace())
    }finally {
      if(ps != null)
        ps.close()
      if(conn != null)
        conn.close()
    }
  }

}
