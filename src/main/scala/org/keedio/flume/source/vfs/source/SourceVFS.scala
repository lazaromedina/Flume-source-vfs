package org.keedio.flume.source.vfs.source

import java.io._
import java.nio.charset.Charset
import java.util
import java.util.concurrent.{ExecutorService, Executors}

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
  val mapOfFiles = mutable.HashMap[String, Long]()
  var sourceVFScounter = new org.keedio.flume.source.vfs.metrics.SourceCounterVfs("")
  val executor: ExecutorService = Executors.newFixedThreadPool(5)
  var sourceName: String = ""
  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {
      event.getState.toString() match {
        case "entry_create" => {
            val thread  = new Thread() {
            override def run(): Unit = {
              LOG.info("listener received event: " + event.getState.toString())
              if (!event.getFileChangeEvent.getFile.exists()) Unit
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
                  mapOfFiles += (fileName -> fileSize)
                  LOG.info("end processing: " + fileName)
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
              if (!event.getFileChangeEvent.getFile.exists()) Unit
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

              if (readStream(inputStream, fileName, prevSize)){
                mapOfFiles += (fileName -> fileSize)
                LOG.info("end processing: " + fileName)
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
    val workDir: String = context.getString("work.dir")
    val includePattern = context.getString("includePattern", """[^.]*\.*?""")
    LOG.debug("Source " + sourceName + " watching path : " + workDir + " and pattern " + includePattern)
    val watchable = new WatchablePath(workDir, 0, 0, s"""$includePattern""".r)
    watchable.addEventListener(listener)
  }

  override def start(): Unit =  {
    super.start()
    sourceVFScounter.start
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
}
