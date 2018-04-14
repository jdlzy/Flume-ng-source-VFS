package org.lzy.flume.source.vfs.watcher

import java.util.concurrent._

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.DefaultFileMonitor
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by luislazaro on 4/9/15.
  * lalazaro@lzy.com
  * lzy
  */

class WatchablePath(uri: String, refresh: Int, start: Int, regex: Regex, fileObject: FileObject, listener:
StateListener, processDiscovered: Boolean, sourceName: String) {

    val LOG: Logger = LoggerFactory.getLogger(classOf[WatchablePath])

    //list of susbcribers(observers) for changes in fileObject
    private val listeners: ListBuffer[StateListener] = new ListBuffer[StateListener]
    //文件目录们
    private val children: Array[FileObject] = fileObject.getChildren
    addEventListener(listener)

    //监控目录的文件变化
    private val fileListener = new VfsFileListener {
        override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit = {
            val eventDelete: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_DELETE)
            if (isValidFilenameAgainstRegex(eventDelete)) {
                fireEvent(eventDelete)
            }
        }

        override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = {
            val eventChanged: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_MODIFY)
            if (isValidFilenameAgainstRegex(eventChanged)) {
                fireEvent(eventChanged)
            }
        }

        override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = {
            val eventCreate: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_CREATE)
            if (isValidFilenameAgainstRegex(eventCreate)) {
                fireEvent(eventCreate)
            }
        }

        override def fileDiscovered(fileChangeEvent: FileChangeEvent): Unit = {
            val eventDiscovered: StateEvent = new StateEvent(fileChangeEvent, State.ENTRY_DISCOVER)
            if (isValidFilenameAgainstRegex(eventDiscovered)) {
                fireEvent(eventDiscovered)
            }
        }
    }

    //Thread based polling file system monitor with a 1 second delay.
    //线程每一秒进行一次获取文件系统监控器
    private val defaultMonitor: DefaultFileMonitor = new DefaultFileMonitor(fileListener)
    //设置监控时间
    defaultMonitor.setDelay(secondsToMiliseconds(refresh))
    //设置是否进行文件目录的递归
    defaultMonitor.setRecursive(true)
    //
    defaultMonitor.addFile(fileObject)
    processDiscovered match {
        case true =>
            children.foreach(child => fileListener.fileDiscovered(new FileChangeEvent(child)))
            LOG.info("Source[" + sourceName + "] 已经将配置项 'process.discovered.files' 设置为[" + processDiscovered + "], " +
                    "在Source启动之前不进行处理")
        case false =>
            LOG.info("Source [" + sourceName + "]已经将配置项 'process.discovered.files' 设置为[" + processDiscovered + "]," +
                    "在Source启动之前不进行处理")
    }

    // 线程池，the number of threads to keep in the pool, even if they are idle
    private val corePoolSize = 1
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize)
    //Creates and executes a one-shot action that becomes enabled after the given delay
    private val tasks: ScheduledFuture[_] = scheduler.schedule(
        getTaskToSchedule(),
        start,
        TimeUnit.SECONDS
    )

    /**
      * Call this method whenever you want to notify the event listeners about a
      * FileChangeEvent.
      * Filtering monitored files via regex is made after an event is fired.
      * 接收数据，
      */
    def fireEvent(stateEvent: StateEvent): Unit = {
        listeners foreach (_.statusReceived(stateEvent))
    }

    /**
      * Check filename string against regex
      * 检查文件名称是否符合正则表达式
      *
      * @param stateEvent
      * @return
      */
    def isValidFilenameAgainstRegex(stateEvent: StateEvent) = {
        val fileName: String = stateEvent.getFileChangeEvent.getFile.getName.getBaseName
        regex.findFirstIn(fileName).isDefined
    }

    /**
      * Add element to list of registered listeners
      * 添加一个监听器，放在监听器列表的最前边
      *
      * @param listener
      */
    def addEventListener(listener: StateListener): Unit = {
        listener +=: listeners
    }

    /**
      * Remove element from list of registered listeners
      * 从监听器列表中删除指定的监听器
      *
      * @param listener
      */
    def removeEventListener(listener: StateListener): Unit = {
        listeners.find(_ == listener) match {
            case Some(listener) => {
                listeners.remove(listeners.indexOf(listener))
            }
            case None => ()
        }
    }

    /**
      *
      * auxiliar for using seconds where miliseconds is requiered
      * 把秒转换为毫秒
      *
      * @param seconds
      * @return
      */
    implicit def secondsToMiliseconds(seconds: Int): Long = {
        seconds * 1000
    }

    /**
      * Make a method runnable and schedule for one-shot
      * 创建一个线程，开始监听
      *
      * @return
      */
    def getTaskToSchedule(): Runnable = {
        new Runnable {
            override def run(): Unit = {
                defaultMonitor.start()
            }
        }
    }

}