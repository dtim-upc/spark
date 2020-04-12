/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.utils

import java.io.{File, FileInputStream, FileOutputStream, InputStream, IOException}
import java.nio.file.{Files, FileSystems, Paths}
import java.util.zip.{ZipEntry, ZipInputStream}

object Unzip {
//  val INPUT_ZIP_FILE: String = "src/main/resources/my-zip.zip"
//  val OUTPUT_FOLDER: String = "src/main/resources/my-zip"

  def unZipIt(zipFile: InputStream): String = {

//    println(System.getProperty("java.io.tmpdir"))
    val outputFolder = Files.createTempDirectory("modelsFJ")
    val buffer = new Array[Byte](1024)

    try {

      // zip file content
      val zis: ZipInputStream = new ZipInputStream(zipFile)
      // get the zipped file list entry
      var ze: ZipEntry = zis.getNextEntry()

      while (ze != null) {
        val fileName = ze.getName()
        if (ze.isDirectory) {
          Files.createDirectories(FileSystems.getDefault().getPath(
            outputFolder + File.separator + fileName))
        } else {

          val newFile = new File(outputFolder + File.separator + fileName)

          // create folders
          new File(newFile.getParent()).mkdirs()
          val fos = new FileOutputStream(newFile)
          var len: Int = zis.read(buffer)
          while (len > 0) {
            fos.write(buffer, 0, len)
            len = zis.read(buffer)
          }
          fos.close()
        }
        ze = zis.getNextEntry()
      }

      zis.closeEntry()
      zis.close()
      outputFolder.toString

    } catch {
      case e: IOException =>
        // scalastyle:off println
        println("exception caught: " + e.getMessage)
        outputFolder.toString
      // scalastyle:on println
    }

  }
}
