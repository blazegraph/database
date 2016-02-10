/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.bigdata.journal;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractJournal.ISnapshotData;
import com.bigdata.quorum.Quorum;

/**
 * Take a snapshot of the journal.
 * 
 * @author bryan
 * 
 * @see <a href="http://trac.bigdata.com/ticket/1080"> Snapshot mechanism breaks
 *      with metabit demi-spaces </a>
 * @see <a href="http://trac.bigdata.com/ticket/1172"> Online backup for Journal
 *      </a>
 */
public class SnapshotTask implements Callable<ISnapshotResult> {

   private final Journal journal;
   private final ISnapshotFactory snapshotFactory;
   
   protected static final Logger log = Logger.getLogger(SnapshotTask.class);
   
   public interface Options {
	   
	  /**
	   * 
	   * Java property to override the default GZIP buffer size used for {@link GZipInputStream} and {@link GZipOutputStream}. 
	   * 
	   * This specifies the size in Bytes to use.  The default is 512k.
	   * 
	   * -Dcom.bigdata.journal.SnapshotTask.gzipBufferSize=512
	   * 
	   * A larger value such as below is recommended for larger files.
	   * 
	   * -Dcom.bigdata.journal.SnapshotTask.gzipBufferSize=65535
	   *  
	   */
	   
	   public static final String GZIP_BUFFER_SIZE = SnapshotTask.class.getClass().getName()+".gzipBufferSize"; 
	   
   }
   
   private static int getGzipBuffer() {
	   
	   final String s = System.getProperty(Options.GZIP_BUFFER_SIZE);
	   
	   if(s == null || s.isEmpty()) {
		   return DEFAULT_BUFFER;
	   } else {
		   return Integer.parseInt(s);
	   }
	   
   }
   
   /**
    * See BLZG-1732
    */
   private static final int GZIP_BUFFER =  getGzipBuffer();
   private static final int DEFAULT_BUFFER = 512;
   
   /**
    * The prefix for the temporary files used to generate snapshots.
    */
   public final static String SNAPSHOT_TMP_PREFIX = "snapshot";
   
   /**
    * The suffix for the temporary files used to generate snapshots.
    */
   public final static String SNAPSHOT_TMP_SUFFIX = ".tmp";

   public SnapshotTask(final Journal journal,
         final ISnapshotFactory snapshotFactory) {
      if (journal == null)
         throw new IllegalArgumentException();
      if (snapshotFactory == null)
         throw new IllegalArgumentException();
      this.journal = journal;
      this.snapshotFactory = snapshotFactory;
   }

   @Override
   public ISnapshotResult call() throws Exception {

      // Grab a read lock.
      final long txId = journal.newTx(ITx.READ_COMMITTED);
      try {

         /*
          * Get all snapshot core data, including rootblocks and any allocation
          * data, setting the current committed rootblock view.
          */
         final AtomicReference<IRootBlockView> rbv = new AtomicReference<IRootBlockView>();
         final ISnapshotData coreData = journal.snapshotAllocationData(rbv);

         if (rbv.get().getCommitCounter() == 0L) {

            throw new IllegalStateException("Journal is empty");

         }

         final File file = snapshotFactory.getSnapshotFile(rbv.get());

         if (file.exists() && file.length() != 0L) {

            /*
             * Snapshot exists and is not (logically) empty.
             * 
             * Note: The SnapshotManager will not recommend taking a snapshot if
             * a snapshot already exists for the current commit point since
             * there is no committed delta that can be captured by the snapshot.
             * 
             * This code makes sure that we do not attempt to overwrite a
             * snapshot if we already have one for the same commit point. If you
             * want to re-generate a snapshot for the same commit point (e.g.,
             * because the existing one is corrupt) then you MUST remove the
             * pre-existing snapshot first.
             */

            throw new IOException("File exists: " + file);

         }

         final File parentDir = file.getParentFile();

         // Make sure the parent directory(ies) exist.
         if (!parentDir.exists())
            if (!parentDir.mkdirs())
               throw new IOException("Could not create directory: " + parentDir);

         /*
          * Create a temporary file. We will write the snapshot here. The file
          * will be renamed onto the target file name iff the snapshot is
          * successfully written.
          */
         final File tmp = File.createTempFile(
               SNAPSHOT_TMP_PREFIX,
               SNAPSHOT_TMP_SUFFIX, parentDir);

         OutputStream osx = null;
         DataOutputStream os = null;
         boolean success = false;
         try {

            osx = new FileOutputStream(tmp);

            if (snapshotFactory.getCompress())
               osx = new GZIPOutputStream(osx, GZIP_BUFFER);

            os = new DataOutputStream(osx);

            // write out the file data.
            ((IHABufferStrategy) journal.getBufferStrategy()).writeOnStream(os,
                  coreData, null/* quorum */, Quorum.NO_QUORUM);

            // flush the output stream.
            os.flush();

            // done.
            success = true;
         } catch (Throwable t) {
            /*
             * Log @ ERROR and launder throwable.
             */
            Journal.log.error(t, t);
            if (t instanceof Exception)
               throw (Exception) t;
            else
               throw new RuntimeException(t);
         } finally {

            if (os != null) {
               try {
                  os.close();
               } finally {
                  // ignore.
                  os = null;
                  osx = null;
               }
            } else if (osx != null) {
               try {
                  osx.close();
               } finally {// ignore
                  osx = null;
               }
            }

            /*
             * Either rename the temporary file onto the target filename or
             * delete the tempoary file. The snapshot is not considered to be
             * valid until it is found under the appropriate name.
             */
            if (success) {

               // if (!journal.getQuorum().getClient().isJoinedMember(token)) {
               // // Verify before putting down the root blocks.
               // throw new QuorumException(
               // "Snapshot aborted: service not joined with met quorum.");
               // }

               if (!tmp.renameTo(file)) {

                  Journal.log.error("Could not rename " + tmp + " as " + file);

               } else {

                  if (Journal.log.isInfoEnabled())
                     Journal.log.info("Captured snapshot: " + file
                           + ", commitCounter=" + rbv.get().getCommitCounter()
                           + ", length=" + file.length());

               }

            } else {

               if (!tmp.delete()) {

                  Journal.log.warn("Could not delete temporary file: " + tmp);

               }

            }

         }

         // Done.
         return new SnapshotResult(file, snapshotFactory.getCompress(),
               rbv.get());
   
      } finally {
         // Release the read lock.
         journal.abort(txId);
      }
   }
   
   /**
    * Copy the input stream to the output stream.
    * 
    * @param content
    *            The input stream.
    * @param outstr
    *            The output stream.
    * 
    * @throws IOException
    */
   static private void copyStream(final InputStream content,
           final OutputStream outstr) throws IOException {

       final byte[] buf = new byte[GZIP_BUFFER];

       while (true) {

           final int rdlen = content.read(buf);

           if (rdlen <= 0) {

               break;

           }

           outstr.write(buf, 0, rdlen);

       }

   }
   
   /**
    * Decompress a snapshot onto the specified file. The original file is not
    * modified.
    * 
    * @param src
    *            The snapshot.
    * @param dst
    *            The file onto which the decompressed snapshot will be written.
    * 
    * @throws IOException
    *             if the source file does not exist.
    * @throws IOException
    *             if the destination file exists and is not empty.
    * @throws IOException
    *             if there is a problem decompressing the source file onto the
    *             destination file.
    */
   public static void decompress(final File src, final File dst)
           throws IOException {

       if (!src.exists())
           throw new FileNotFoundException(src.getAbsolutePath());

       if (dst.exists() && dst.length() != 0)
           throw new IOException("Output file exists and is not empty: "
                   + dst.getAbsolutePath());

       if (log.isInfoEnabled())
           log.info("src=" + src + ", dst=" + dst);

       InputStream is = null;
       OutputStream os = null;
       try {
    	   //BLZG-1732:   
           is = new GZIPInputStream(new FileInputStream(src), GZIP_BUFFER);
           os = new BufferedOutputStream(new FileOutputStream(dst));
           copyStream(is, os);
           os.flush();
       } finally {
           if (is != null)
               try {
                   is.close();
               } catch (IOException ex) {
               }
           if (os != null)
               try {
                   os.close();
               } catch (IOException ex) {
               }
       }

   }
   
} // class SnapshotTask
