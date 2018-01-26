
package org.keedio.flume.source.vfs.metrics

import org.apache.flume.instrumentation.MonitoredCounterGroup
import org.joda.time.{DateTime, Period}



class SourceCounterVfs(val name: String)
  extends MonitoredCounterGroup(MonitoredCounterGroup.Type.SOURCE, name,
    Seq(
      "files_count",
      "eventCount",
      "start_time",
      "last_sent",
      "throughput",
      "bytesProcessed",
      "KbProcessed",
      "MbProcessed"):_*)
    with SourceCounterVfsMBean {
  private var files_count = 0L
  private var eventCount = 0L
  private var throughput = 0L
  private var start_time = System.currentTimeMillis
  private var last_sent = 0L
  private var bytesProcessed = 0L
  private var KbProcessed = 0L
  private var MbProcessed: Double = 0
  var ATTRIBUTES = Seq(
    "files_count",
    "eventCount",
    "start_time",
    "last_sent",
    "throughput",
    "bytesProcessed",
    "KbProcessed",
    "MbProcessed"
  )


  /**
    * @return long, number of files discovered
    */
  override def getFilesCount: Long = files_count

  override def incrementEventCount(): Unit = {
    last_sent = System.currentTimeMillis
    eventCount += 1
    if (last_sent - start_time >= 1000) {
      val secondsElapsed = (last_sent - start_time) / 1000
      throughput = eventCount / secondsElapsed
    }
  }

  /**
    *
    * @return long
    */
  override def getEventCount: Long = eventCount

  override def incrementFilesCount(): Unit = files_count += 1

  override def getThroughput: Long = throughput

  override def getLastSent: Long = last_sent

  override def getStarTime: Long = start_time

  override def getLastSent_Human: String = {
    val dateTime = new DateTime(last_sent)
    val date = dateTime.toString("YYYY-MM-dd_HH:mm:ss.SSS")
    date
  }

  override def getStartTime_Human: String = {
    val dateTime = new DateTime(start_time)
    val date = dateTime.toString("YYYY-MM-dd_HH:mm:ss.SSS")
    date
  }

  override def incrementCountSizeProc(size: Long): Unit = bytesProcessed += size

  override def getCountSizeProcBytes: Long = bytesProcessed

  override def getCountSizeProcKb: Long = {
    KbProcessed = getCountSizeProcBytes / 1024
    KbProcessed
  }

  override def getCountSizeProcMb: Double = {
    MbProcessed = getCountSizeProcBytes.toDouble / (1024 * 1024)
    MbProcessed
  }

  override def getElapsedTime: String = {
     (eventCount > 0) match {
       case true => {
         val period = new Period( new DateTime(start_time), new DateTime())
         new String(period.getDays + " " +
           period.getHours + ":" + period.getMinutes + ":" + period.getSeconds)
       }
       case false => "no events"
    }
  }

}