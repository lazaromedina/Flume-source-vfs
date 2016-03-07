package org.keedio.flume.source.vfs.watcher

import java.net.URI

import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder
import org.apache.commons.vfs2.{FileObject, FileSystemOptions, VFS}
/**
  * Created by luislazaro on 7/3/16.
  * lalazaro@keedio.com
  * Keedio
  */
/**
  * VFS2 supported file systems requiere is some cases to
  * specify config parameters
  */
object FileObjectBuilder {

  private val fsManager = VFS.getManager
  private val options: FileSystemOptions = new FileSystemOptions()

  /**
    * Get a FileObject for the supported file system.
    * @param uri
    * @return
    */
  def getFileObject(uri: String): FileObject = {
    val scheme = getScheme(uri)
    scheme match {
      case "ftp" => {
        val builder = FtpFileSystemConfigBuilder.getInstance()
        builder.setUserDirIsRoot(options, false)
        builder.setPassiveMode(options, true) //set to true if behind firewall
        fsManager.resolveFile(uri, options)
      }
      case _ => fsManager.resolveFile(uri)
    }
  }

  /**
    * Get the scheme of an URI.
    * @param uriString
    * @return
    */
  def getScheme(uriString: String): String = {
    val uri = new URI(uriString)
    uri.getScheme
  }

}
