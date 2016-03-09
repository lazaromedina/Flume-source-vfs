package org.keedio.flume.source.vfs.source

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.Charset
import java.util

import org.apache.flume.conf.Configurable
import org.apache.flume.event.SimpleEvent
import org.apache.flume.source.AbstractSource
import org.apache.flume.{ChannelException, Context, Event, EventDrivenSource}
import org.keedio.flume.source.vfs.watcher.{StateEvent, StateListener, WatchablePath}
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

  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {

      LOG.info("listener received event: " + event.getState.toString())

      event.getState.toString() match {
        case "entry_create" => {
          val thread: Thread = new Thread() {
            if (!event.getFileChangeEvent.getFile.exists()) Unit
            if (!event.getFileChangeEvent.getFile.isReadable) {
              LOG.error(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable")
              Unit
            }
            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
            val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
            val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream

            override def run(): Unit = {
              if (event.getFileChangeEvent.getFile.isFile) {
                LOG.info("total threads " + Thread.activeCount() + " " + Thread.currentThread().getName +
                  " started processing file: " + fileName)
                if (readStream(inputStream, fileName, 0)) {
                  mapOfFiles += (fileName -> fileSize)
                  LOG.info("end processing: " + fileName)
                }
              }
            }
          }
          thread.start()
          Thread.sleep(50)
        }

        case "entry_modify" => {
          val thread: Thread = new Thread() {
            if (!event.getFileChangeEvent.getFile.exists()) Unit
            if (!event.getFileChangeEvent.getFile.isReadable) {
              LOG.error(event.getFileChangeEvent.getFile.getPublicURIString + " is not readable")
              Unit
            }
            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName
            val fileSize = event.getFileChangeEvent.getFile.getContent.getSize
            val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream

            override def run(): Unit = {
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
          thread.start()
          Thread.sleep(50)
        }

        case "entry_delete" => {
          val thread: Thread = new Thread() {

            val fileName = event.getFileChangeEvent.getFile.getName.getBaseName

            override def run(): Unit = {
              mapOfFiles -= fileName
            }

            LOG.info("total threads " + Thread.activeCount() + " " + Thread.currentThread().getName + " Deleted: " +
              fileName)
          }
          thread.start
          Thread.sleep(50)
        }

        case _ => LOG.error("Recieved event is not register")
      }
    }
  }


  override def configure(context: Context): Unit = {
    val workDir: String = context.getString("work.dir");
    val watchable = new WatchablePath(workDir, 0, 2, """[^.]*\.*?""".r)
    watchable.addEventListener(listener)
  }


  override def start(): Unit = super.start()

  override def stop(): Unit = super.stop()

  def processMessage(data: Array[Byte]): Unit = {
    val event: Event = new SimpleEvent
    val headers: java.util.Map[String, String] = new util.HashMap[String, String]()
    event.setBody(data)
    event.setHeaders(headers)
    try {
      getChannelProcessor.processEvent(event)
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
      in => processMessage(in.getBytes())
    }
    in.close()
    true
  }


}
