package org.keedio.flume.source.vfs

import org.apache.commons.vfs2._
import org.apache.commons.vfs2.impl.{StandardFileSystemManager, _}
import org.apache.commons.vfs2.provider.ftp._


/**
 * Created by luislazaro on 8/9/15.
 * lalazaro@keedio.com
 * Keedio
 */
class DefaultMonitorFTPTest {

      /**
       * Test vfs2 on FTP filesytem
       */
      //@Test
      def testApiFileMonitorFTPFileSystem(): Unit = {
            val fsManager = new StandardFileSystemManager()
            fsManager.setCacheStrategy(CacheStrategy.ON_RESOLVE)
            fsManager.init()
            val opts: FileSystemOptions = new FileSystemOptions()
            val builder: FtpFileSystemConfigBuilder = FtpFileSystemConfigBuilder.getInstance()
            builder.setUserDirIsRoot(opts, false)
            builder.setPassiveMode(opts, false)

            //just accesing ftp server
            //val fileObject = fsManager.resolveFile("ftp://mortadelo:filemon@pilum/home/mortadelo/ftp", opts)
            //val fileObject = fsManager.resolveFile("ftp://mortadelo:mortadelo@10.129.135.122/subdirA", opts)
            //val foc = new OnCallRefreshFileObject(fileObject)

//            val children = fileObject.getChildren
//            children.foreach(f => println(f.getName.getBaseName))

            //val file1 = fsManager.resolveFile("ftp://mortadelo:filemon@pilum/home/mortadelo/ftp/file4.txt", opts)
            //val file1 = fsManager.resolveFile("ftp://mortadelo:mortadelo@10.129.135.122/file4.txt", opts)

            //monitoring
            //val listendir: FileObject = fsManager.resolveFile("ftp://mortadelo:filemon@pilum/home/mortadelo/ftp", opts)
            val listendir: FileObject = fsManager.resolveFile("ftp://mortadelo:mortadelo@10.129.135.122/subdirA", opts)

            val defaultFileMonitor: DefaultFileMonitor = new DefaultFileMonitor(new FileListener {
                  override def fileDeleted(fileChangeEvent: FileChangeEvent): Unit =
                        println("file deleted: " + fileChangeEvent.getFile.getName.getBaseName)

                  override def fileChanged(fileChangeEvent: FileChangeEvent): Unit = {
                        println("file changed: " + fileChangeEvent.getFile.getName.getBaseName + " " +
                          fileChangeEvent.getFile.getContent.getLastModifiedTime + " "
                          + fileChangeEvent.getFile.getContent.getFile.isContentOpen + " "
                        + fileChangeEvent.getFile.getContent.getSize)
                  }

                  override def fileCreated(fileChangeEvent: FileChangeEvent): Unit = {
                        println("file created: " + fileChangeEvent.getFile.getName.getBaseName)
                  }
            })

            defaultFileMonitor.setRecursive(true)
            defaultFileMonitor.addFile(listendir)
            defaultFileMonitor.setDelay(5000) //if not set or set to 0 seconds, file changed is not fired so it is not detected.
            defaultFileMonitor.start()
            for (i <- 1 to 1000) {
                  listendir.refresh()
                  Thread.sleep(1000)
            }


//          try {
//                  file1.createFile()
//                  Thread.sleep(1000)
//                  //file1.delete()
//            } catch {
//                  case e: IOException => println("I/O: ", e)
//            }
            //defaultFileMonitor.stop()

      }
}
