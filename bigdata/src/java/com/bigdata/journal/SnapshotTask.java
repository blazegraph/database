/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import com.bigdata.journal.AbstractJournal.ISnapshotData;
import com.bigdata.journal.jini.ha.SnapshotManager;
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
class SnapshotTask implements Callable<ISnapshotResult> {

   private final Journal journal;
   private final ISnapshotFactory snapshotFactory;

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
               SnapshotManager.SNAPSHOT_TMP_PREFIX,
               SnapshotManager.SNAPSHOT_TMP_SUFFIX, parentDir);

         OutputStream osx = null;
         DataOutputStream os = null;
         boolean success = false;
         try {

            osx = new FileOutputStream(tmp);

            if (snapshotFactory.getCompress())
               osx = new GZIPOutputStream(osx);

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
   
} // class SnapshotTask