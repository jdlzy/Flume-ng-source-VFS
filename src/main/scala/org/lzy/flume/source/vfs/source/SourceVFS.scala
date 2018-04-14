package org.lzy.flume.source.vfs.source

import java.io._
import java.nio.charset.Charset
import java.nio.file._
import java.util
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.commons.io.FileUtils
import org.apache.commons.vfs2.FileObject
import org.apache.flume.conf.Configurable
import org.apache.flume.event.SimpleEvent
import org.apache.flume.source.AbstractSource
import org.apache.flume.{ChannelException, Context, Event, EventDrivenSource}
import org.lzy.flume.source.vfs.metrics.SourceCounterVfs
import org.lzy.flume.source.vfs.watcher._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * 功能描述: Flume的VFS数据源，可以支持本地以及ftp数据源
  * ftp数据源不支持将文件处理之后，将其转存
  *
  * Author lzy
  * Date 2018/4/14 11:13
  */
class SourceVFS extends AbstractSource with Configurable with EventDrivenSource {

    val LOG: Logger = LoggerFactory.getLogger(classOf[SourceVFS])
    var mapOfFiles = mutable.HashMap[String, Long]()
    var sourceVFScounter = new org.lzy.flume.source.vfs.metrics.SourceCounterVfs("")
    val executor: ExecutorService = Executors.newFixedThreadPool(10)
    //源名称
    var sourceName: String = ""
    //文件状态
    var statusFile = ""
    //监控目录
    var workDir: String = ""
    //文件名称过滤正则表达式
    var includePattern: String = ""
    //传送完毕的文件被移动到该目录下。
    var processedDir: String = ""
    //
    var processDiscovered: Boolean = true

    /**
      * 配置一个监听器
      */
    val listener = new StateListener {
        override def statusReceived(event: StateEvent): Unit = {
            //获取当前事件的状态
            event.getState.toString() match {
//事件新建
                case "entry_create" => {
                    val thread = new Thread() {
                        override def run(): Unit = {
                            if (!isEventValidStatus(event)) {
                                Unit
                            }
                            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
                            LOG.info("Source[" + sourceName + "]接收到事件: " + event.getState.toString() + "文件+ fileName)")
                            val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
                            val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
                            LOG.info("线程[" + Thread.currentThread().getName + "]开始处理新文件: " + fileName)
                            if (readStream(inputStream, fileName, 0)) {
                                LOG.info("结束文件处理: " + fileName)
                                mapOfFiles += (fileName -> fileSize)
                                saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                                sourceVFScounter.incrementFilesCount()
                                sourceVFScounter.incrementCountSizeProc(fileSize)
                                if (Files.exists(Paths.get(processedDir))) {
                                    //默认目标目录如果不存在不会拷贝
//                                    FileUtils.moveFileToDirectory(new File(workDir + "/" + fileName), new File(processedDir), false)
                                    //修改为如果目录不存在，那么创建该目录，并移动
                                    FileUtils.moveFileToDirectory(new File(workDir + "/" + fileName), new File(processedDir), true)
                                    LOG.info("移动文件[" + fileName + "]到指定目录->" + processedDir)
                                }
                            }
                        }
                    }
                    executor.execute(thread)
                } //entry_create

                case "entry_modify" => {
                    val thread = new Thread() {
                        override def run(): Unit = {
                            if (!isEventValidStatus(event)) {
                                Unit
                            }
                            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
                            LOG.info("Source " + sourceName + " received event: " + event.getState.toString() + " file " + fileName)
                            val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
                            val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
                            val prevSize = mapOfFiles.getOrElse(fileName, fileSize)
                            LOG.info("File exists in map of files, previous size of file is " + prevSize + ", " + Thread
                                    .currentThread().getName + " started processing modified file: " + fileName)
                            if (readStream(inputStream, fileName, prevSize)) {
                                LOG.info("End processing modified file: " + fileName)
                                mapOfFiles -= fileName
                                mapOfFiles += (fileName -> fileSize)
                                saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                            }
                        }
                    }
                    executor.execute(thread)
                } //entry_modify

                case "entry_delete" => {
                    val thread: Thread = new Thread() {
                        override def run(): Unit = {
                            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
                            LOG.info("Source " + sourceName + " received event: " + event.getState.toString() + " file " + fileName)

                            /*
                                    todo: if file is deleted from directory
                                    option 1) keep as file deleted in map of files
                                    option 2) delete from map of files
                            */

//              mapOfFiles -= fileName
//              saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                        }
                    }
                    executor.execute(thread)
                } //entry_delete

                case "entry_discover" => {
                    val thread = new Thread() {
                        override def run(): Unit = {
                            if (!isEventValidStatus(event)) {
                                Unit
                            }
                            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
                            LOG.info("Source " + sourceName + " received event: " + event.getState.toString() + " file " + fileName)
                            val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
                            val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
                            mapOfFiles.get(fileName).isDefined match {
                                case false =>
                                    LOG.info(Thread.currentThread().getName + " started processing file discovered: " + fileName)
                                    if (readStream(inputStream, fileName, 0)) {
                                        LOG.info("End processing discovered file: " + fileName)
                                        mapOfFiles += (fileName -> fileSize)
                                        saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                                        sourceVFScounter.incrementFilesCount()
                                        sourceVFScounter.incrementCountSizeProc(fileSize)
                                        if (Files.exists(Paths.get(processedDir))) {
                                            FileUtils.moveFileToDirectory(new File(workDir + "/" + fileName), new File(processedDir), false)
                                            LOG.info("Moving processed file " + fileName + " to dir " + processedDir)
                                        }
                                    }

                                case true => {
                                    val prevSize = mapOfFiles.getOrElse(fileName, fileSize)
                                    if (prevSize == fileSize) {
                                        LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                                                .currentThread().getName + " nothing to do, file remains unchanged " + fileName)
                                        Unit
                                    } else {
                                        LOG.info("File exists in map of files, previous size of file is " + prevSize + " " + Thread
                                                .currentThread().getName +
                                                " started processing modified file: " + fileName)
                                        if (readStream(inputStream, fileName, prevSize)) {
                                            LOG.info("End processing modified file: " + fileName)
                                            mapOfFiles -= fileName
                                            mapOfFiles += (fileName -> fileSize)
                                            saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                                        }
                                    }
                                    if (Files.exists(Paths.get(processedDir))) {
                                        FileUtils.moveFileToDirectory(new File(workDir + "/" + fileName), new File(processedDir), false)
                                        LOG.info("Moving processed file " + fileName + " to dir " + processedDir)
                                    }
                                }
                            }
                        }
                    }
                    executor.execute(thread)
                } //entry_discover

                case _ => LOG.error("接收的事件未被注册.")
            }
        }
    }

    override def configure(context: Context): Unit = {
        sourceName = this.getName
        sourceVFScounter = new SourceCounterVfs("SOURCE." + sourceName)
        workDir = context.getString("work.dir")
        includePattern = context.getString("includePattern", """[^.]*\.*?""")
        LOG.info("Source[" + sourceName + "]监控路径 : " + workDir + "，过滤文件的正则表达式为：" + includePattern)
        processedDir = context.getString("processed.dir", null)
        statusFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + sourceName + ".ser"
        processDiscovered = context.getString("process.discovered.files", "true").toBoolean
        if (Files.exists(Paths.get(statusFile))) {
            mapOfFiles = loadMap(statusFile)
        }
    }

    /**
      * 功能实现:
      *
      * Aauthor: Lzy
      * Date: 2018/4/14 14:11
      * Param: []
      * Return: void
      */
    override def start(): Unit = {
        super.start()
        sourceVFScounter.start
        val fileObject: FileObject = FileObjectBuilder.getFileObject(workDir)
        //创建监控器，每5s刷新一次，调用线程延迟2s
        val watchable = new WatchablePath(workDir, 5, 2, s"""$includePattern""".r, fileObject, listener, processDiscovered, sourceName)
    }

    override def stop(): Unit = {

        sourceVFScounter.stop()
        super.stop()
    }

    /**
      * 功能实现:实现处理数据，并将其传送
      *
      * Aauthor: Lzy
      * Date: 2018/4/14 14:10
      * Param: [data, fileName]
      * Return: void
      */
    def processMessage(data: Array[Byte], fileName: String): Unit = {
        val event: Event = new SimpleEvent
        val headers: java.util.Map[String, String] = new util.HashMap[String, String]()
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        val flag = fileName.split("_")(0)
        headers.put("fileName", fileName);
        headers.put("flag", flag)
        event.setBody(data)
        event.setHeaders(headers)
        try {
            getChannelProcessor.processEvent(event)
            sourceVFScounter.incrementEventCount()
        } catch {
            case ex: ChannelException => {
                Thread.sleep(2000)
            }
        }
    }

    /**
      * 功能实现:读取输入流，并将其转换为字节数组， 将其发送到Flume
      *
      * Aauthor: Lzy
      * Date: 2018/4/14 14:11
      * Param: [inputStream, filename, size]
      * Return: boolean
      */
    def readStream(inputStream: InputStream, filename: String, size: Long): Boolean = {
        if (inputStream == null) {
            return false
        }
        inputStream.skip(size)
        val in: BufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))

        Stream.continually(in.readLine()).takeWhile(_ != null) foreach {
            in => processMessage(in.getBytes(), filename)
        }

        in.close()
        true
    }

    /**
      * 功能实现:将已经处理过得文件写入文件系统
      *
      * Author: Lzy
      * Date: 2018/4/14 14:13
      * Param: [mapOfFiles, statusFile, fileName, state]
      * Return: boolean
      */
    def saveMap(mapOfFiles: mutable.Map[String, Long], statusFile: String, fileName: String, state: String): Boolean = {
        val oos = new ObjectOutputStream(new FileOutputStream(statusFile))
        try {
            oos.writeObject(mapOfFiles)
            oos.close()
            if (LOG.isDebugEnabled) {
                LOG.debug("Write to map of files " + state + ": " + fileName)
            }

            LOG.info("Write to map of files " + state + ": " + fileName)
            true
        } catch {
            case io: IOException =>
                LOG.error("Cannot write object " + mapOfFiles + " to " + statusFile, io)
                false
        }
    }

    /**
      * 功能实现:从文件系统Map中加载以及处理的文件
      *
      * Author: Lzy
      * Date: 2018/4/14 14:14
      * Param: [statusFile]
      * Return: scala.collection.mutable.HashMap<java.lang.String,java.lang.Object>
      */
    def loadMap(statusFile: String): mutable.HashMap[String, Long] = {
        val ois = new ObjectInputStream(new FileInputStream(statusFile))
        val mapOfFiles = ois.readObject().asInstanceOf[mutable.HashMap[String, Long]]
        ois.close()
        if (LOG.isDebugEnabled) {
            LOG.debug("Load from file system map of processed files. " + statusFile)
        }
        mapOfFiles
    }

    /**
      * 功能实现:时间状态是否有效
      *
      * Author: Lzy
      * Date: 2018/4/14 14:15
      * Param: [event]
      * Return: boolean
      */
    def isEventValidStatus(event: StateEvent): Boolean = {
        var status = true
        if (!event.getFileChangeEvent.getFile.exists()) {
            LOG.error("File for event " + event.getState + " not exists.")
            status = false
        }
        if (!event.getFileChangeEvent.getFile.isReadable) {
            LOG.error(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable.")
            status = false
        }
        if (!event.getFileChangeEvent.getFile.isFile) {
            LOG.error(event.getFileChangeEvent.getFile.getName.getBaseName + " is not a regular file.")
            status = false
        }
        status
    }

}
