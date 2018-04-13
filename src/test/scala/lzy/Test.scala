package lzy

import java.net.{URI, URL}

/**
  * Created by Administrator on 2018/4/13.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val str="http://www.baidu.com"
    val url=new URI(str)
    println(url.getScheme)
  }
}
