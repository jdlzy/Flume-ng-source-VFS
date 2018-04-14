package org.lzy.flume.source.vfs.utils

import java.sql.DriverManager

import org.lzy.utils.engine.Loader
/**
  *
  * version: 1.0.0
  * Created by licheng on 2018/3/30.
  */
object JDBCUtil {
  val driver = "com.mysql.jdbc.Driver"
  val conf = Loader.pro
  val url = conf.get("jdbc.url")
  val username = conf.get("jdbc.username")
  val password = conf.get("jdbc.password")

  def getConnection() = {
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }

  /**
    * 根据批次以及表名进行数据删除
    * @param inputBatch
    * @param tableName
    */
  def deleteByInputBatch(inputBatch:String,tableName:String):Boolean={
    val conn=getConnection()
    val st=conn.createStatement();
    val sql=s"delete from ${tableName} where INPUT_BATCH='${inputBatch}'"
    val isSuccess=st.execute(sql)
    st.close()
    conn.close()
    isSuccess
  }

  def exeSql(sqlText: String) {
    val conn = getConnection()
    val prpe = conn.createStatement()
    try {
      println(sqlText)
      prpe.execute(sqlText)
    } finally {
      prpe.close()
      conn.close()
    }
  }



}
