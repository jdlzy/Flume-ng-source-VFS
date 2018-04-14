package org.lzy.flume.source.vfs.watcher

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
/**
  * Available file's states.
  */
class State(name: String) {
  override def toString(): String = {
    name
  }
}

object State extends Serializable {
  //创建
  final val ENTRY_CREATE: State = new State("entry_create")
  //删除
  final val ENTRY_DELETE: State = new State("entry_delete")
  //改变
  final val ENTRY_MODIFY: State = new State("entry_modify")
  //发现,监控文件内容是否发生了变化
  final val ENTRY_DISCOVER: State = new State("entry_discover")
}
