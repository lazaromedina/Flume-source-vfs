package org.keedio.flume.source.vfs.source

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.Charset
import java.util

import org.apache.flume.PollableSource.Status
import org.apache.flume.conf.Configurable
import org.apache.flume.event.SimpleEvent
import org.apache.flume.source.AbstractSource
import org.apache.flume.{Context, Event, PollableSource}
import org.keedio.flume.source.vfs.watcher.{StateEvent, StateListener, WatchablePath}
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
class SourceVFS extends AbstractSource with Configurable with PollableSource {

  val LOG: Logger = LoggerFactory.getLogger(classOf[SourceVFS])

  val listener = new StateListener {
    override def statusReceived(event: StateEvent): Unit = {
      LOG.info("listener received event: "
        + event.getState.toString()
        + " on element "
        + event.getFileChangeEvent.getFile.getName)

      val filename = event.getFileChangeEvent.getFile.getName.getBaseName
      val inputStream = event.getFileChangeEvent.getFile.getContent.getInputStream

      if (event.getState.toString().equals("entry_create"))
      readStream(inputStream, filename)
      if (event.getState.toString().equals("entry_modify"))
        readStream(inputStream, filename)
    }
  }


  override def configure(context: Context): Unit = {
    val workDir : String = context.getString("work.dir");
    val watchable = new WatchablePath(workDir, 2, 2, """[^.]*\.csv?""".r)
    watchable.addEventListener(listener)
  }

  override def process(): Status = {
    Status.READY
  }

  override def start(): Unit = super.start()

  override def stop(): Unit = super.stop()

  def processMessage(data: Array[Byte]): Unit = {
    val event: Event = new SimpleEvent
    val headers: java.util.Map[String, String] = new util.HashMap[String, String]()
    event.setBody(data)
    event.setHeaders(headers)
    getChannelProcessor.processEvent(event)
  }

  def readStream(inputStream: InputStream, filename: String): Boolean = {
    if (inputStream == null) {
      return false
    }
    val in: BufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()))
    var line: String = null

    Stream.continually(in.readLine()).takeWhile(_ != null) foreach {
      line => processMessage(line.getBytes())
    }
    in.close()
    true
  }

}
