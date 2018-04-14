package org.lzy.flume.source.vfs.watcher

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@lzy.com
  * lzy
  */
/**
  * Mix-in this trait to become a listener, implementing
  * what to do when an event ins received.
  */
trait StateListener {
  def statusReceived(stateEvent: StateEvent): Unit
}