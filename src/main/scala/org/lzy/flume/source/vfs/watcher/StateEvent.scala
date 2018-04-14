package org.lzy.flume.source.vfs.watcher

import org.apache.commons.vfs2.FileChangeEvent

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@lzy.com
  * lzy
  */
class StateEvent(fileChangeEvent: FileChangeEvent, state: State) {
  def getState = state
  def getFileChangeEvent = fileChangeEvent
}
