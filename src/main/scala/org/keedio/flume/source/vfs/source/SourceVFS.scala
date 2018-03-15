package org.keedio.flume.source.vfs.source

import java.io._
import java.nio.charset.Charset
import java.nio.file._
import java.util
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.commons.vfs2.FileObject
import org.apache.flume.conf.Configurable
import org.apache.flume.event.SimpleEvent
import org.apache.flume.source.AbstractSource
import org.apache.flume.{ChannelException, Context, Event, EventDrivenSource}
import org.keedio.flume.source.vfs.watcher._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
class SourceVFS extends AbstractSource with Configurable with EventDrivenSource {

  val LOG: Logger = LoggerFactory.getLogger(classOf[SourceVFS])
  var mapOfFiles = mutable.HashMap[String, Long]()
  var sourceVFScounter = new org.keedio.flume.source.vfs.metrics.SourceCounterVfs("")
  val executor: ExecutorService = Executors.newFixedThreadPool(5)
  var sourceName: String = ""
  var statusFile = ""
  var workDir: String = ""
  var includePattern: String = ""

  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {
      event.getState.toString() match {
        case "entry_create" => {
          val thread = new Thread() {
            override def run(): Unit = {
              LOG.info("listener received event: " + event.getState.toString())
              if (!event.getFileChangeEvent.getFile.exists()) {
                Unit
              }
              if (!event.getFileChangeEvent.getFile.isReadable) {
                LOG.error(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable")
                Unit
              }
              val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
              val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
              val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream

              if (event.getFileChangeEvent.getFile.isFile) {
                LOG.info("total threads " + Thread.activeCount() + " " + Thread.currentThread().getName +
                  " started processing file: " + fileName)
                if (readStream(inputStream, fileName, 0)) {
                  LOG.info("End processing new file: " + fileName)
                  mapOfFiles += (fileName -> fileSize)
                  saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
                  sourceVFScounter.incrementFilesCount()
                  sourceVFScounter.incrementCountSizeProc(fileSize)
                }
              }
            }
          }
          executor.execute(thread)
        }

        case "entry_modify" => {
          val thread = new Thread() {
            override def run(): Unit = {
              if (!event.getFileChangeEvent.getFile.exists()) {
                Unit
              }
              if (!event.getFileChangeEvent.getFile.isReadable) {
                LOG.error(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable")
                Unit
              }
              val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
              val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
              val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream
              val prevSize = mapOfFiles.getOrElse(fileName, fileSize)
              LOG.info("previous size of file is " + prevSize)
              LOG.info("total threads " + Thread.activeCount() + " " + Thread.currentThread().getName +
                " started processing file: " + fileName)

              if (readStream(inputStream, fileName, prevSize)) {
                LOG.info("End processing modified file: " + fileName)
                mapOfFiles -= fileName
                mapOfFiles += (fileName -> fileSize)
                saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
              }
            }
          }
          executor.execute(thread)
        }

        case "entry_delete" => {
          val thread: Thread = new Thread() {
            override def run(): Unit = {
              val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
              mapOfFiles -= fileName
              saveMap(mapOfFiles, statusFile, fileName, event.getState.toString())
              LOG.info("total threads " + Thread.activeCount() + " " + Thread.currentThread().getName + " Deleted: " +
                fileName)
            }
          }
          executor.execute(thread)
        }
        case _ => LOG.error("Recieved event is not register")
      }
    }
  }

  override def configure(context: Context): Unit = {
    sourceName = this.getName
    sourceVFScounter = new org.keedio.flume.source.vfs.metrics.SourceCounterVfs("SOURCE." + sourceName)
    workDir = context.getString("work.dir")
    includePattern = context.getString("includePattern", """[^.]*\.*?""")
    LOG.debug("Source " + sourceName + " watching path : " + workDir + " and pattern " + includePattern)
//    val fileObject: FileObject = FileObjectBuilder.getFileObject(workDir)
//    val watchable = new WatchablePath(workDir, 5, 2, s"""$includePattern""".r, fileObject, listener)
    //watchable.addEventListener(listener)
    statusFile = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + sourceName + ".ser"
    if (Files.exists(Paths.get(statusFile))) {
      mapOfFiles = loadMap(statusFile)
    }
  }

  override def start(): Unit = {
    super.start()
    sourceVFScounter.start
    val fileObject: FileObject = FileObjectBuilder.getFileObject(workDir)
    val watchable = new WatchablePath(workDir, 5, 2, s"""$includePattern""".r, fileObject, listener)
  }

  override def stop(): Unit = {
    sourceVFScounter.stop()
    super.stop()
  }

  def processMessage(data: Array[Byte], fileName: String): Unit = {
    val event: Event = new SimpleEvent
    val headers: java.util.Map[String, String] = new util.HashMap[String, String]()
    headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
    headers.put("fileName", fileName);
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
    * Write to file system a map of processed files.
    *
    * @param mapOfFiles
    * @param statusFile
    * @return
    */
  def saveMap(mapOfFiles: mutable.Map[String, Long], statusFile: String, fileName: String, state: String): Boolean = {
    val oos = new ObjectOutputStream(new FileOutputStream(statusFile))
    try {
      oos.writeObject(mapOfFiles)
      oos.close()
      LOG.debug("Write to map of files " + state + ": " + fileName)
      true
    } catch {
      case io: IOException =>
        LOG.error("Cannot write object " + mapOfFiles + " to " + statusFile, io)
        false
    }
  }

  /**
    * Load from file system map of processed files.
    *
    * @param statusFile
    * @return
    */
  def loadMap(statusFile: String): mutable.HashMap[String, Long] = {
    val ois = new ObjectInputStream(new FileInputStream(statusFile))
    val mapOfFiles = ois.readObject().asInstanceOf[mutable.HashMap[String, Long]]
    ois.close()
    mapOfFiles
  }

}
