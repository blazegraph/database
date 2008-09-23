/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
/*
 * Created on Sep 19, 2008
 */

package com.bigdata.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.Options;
import com.bigdata.resources.StoreManager;

/**
 * Utility methods for managing exlusive {@link FileLock}s and advisory locks
 * depending on what is supported by the platform, file access mode, and volume
 * on which the file resides.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileLockUtility {

    protected static final Logger log = Logger.getLogger(FileLockUtility.class);
    protected static final boolean INFO = log.isInfoEnabled();
    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * Create/open the file and obtain an exclusive lock.
     * <p>
     * A {@link FileLock} will be used when supported and requested. An advisory
     * lock will be used if <i>useFileLock == false</code>, if the <i>fileMode</i>
     * is read-only. If tryLock() returns <code>null</code> then the lock
     * exists and this request will fail. However, if
     * {@link FileChannel#tryLock()} throws an {@link IOException} then the
     * underlying platform does not support {@link FileLock} for the named file
     * (memory mapped files, read-only files, and NFS mounted files can all have
     * this problem) and we will attempt to acquire an advisory lock instead.
     * 
     * <strong>Advisory locks are NOT visible to other applications</strong>
     * 
     * <p>
     * Do NOT request a {@link FileLock} if you are going to use a memory-mapped
     * buffer. The JDK cautions that these things do not play well together on
     * some platforms.
     * 
     * @param file
     *            The file.
     * @param fileMode
     *            The file mode for
     *            {@link RandomAccessFile#RandomAccessFile(File, String)}
     * @param useFileLock
     *            <code>true</code> if {@link FileChannel#tryLock()} should be
     *            attempted. when <code>false</code> only an advisory lock
     *            will be sought.
     * 
     * @return The {@link RandomAccessFile}
     * 
     * @throws IOException
     *             If the file could not be opened or someone already holds a
     *             lock for that file.
     * 
     * @see FileMetadata#acquireAdvisoryLock(File)
     * 
     * @see Options#FILE_LOCK_ENABLED
     * 
     * @todo we really don't need locks for temporary files.
     * 
     * @todo handle lock files during {@link StoreManager} startup.
     * 
     * @todo if you use a {@link FileLock} to open a file then you are
     *       protected.
     * 
     * @todo this ignores locks for read only file modes. in fact we should
     *       create the file with a read-only marker and allow more than one
     *       process to obtain that lock.
     */
    public static RandomAccessFile openFile(File file, String fileMode,
            boolean useFileLock) throws IOException {
        
        final boolean readOnly = "r".equals(fileMode);
    
        final RandomAccessFile raf = new RandomAccessFile(file, fileMode);
        
        if(readOnly) return raf;
    
        // Note: a System property.
        final boolean fileLockEnabled = Boolean.parseBoolean(System
                .getProperty(Options.FILE_LOCK_ENABLED,
                        Options.DEFAULT_FILE_LOCK_ENABLED));
    
        if (useFileLock && fileLockEnabled) {//bufferMode != BufferMode.Mapped) {
    
            if (INFO)
                log.info("Seeking exclusive lock: " + file.getAbsolutePath());
            
            if (new File(file + ".lock").exists()) {

                // reject if there is already an advisory lock for this file.
                
                throw new IOException("Advisory lock exists: "
                        + file.getAbsolutePath());
                
            }
            
            try {
                
                // seek a native platform exclusive file lock.
                if (raf.getChannel().tryLock() != null) {
    
                    // got it.
                    return raf;
                    
                } else {
                    
                    /*
                     * A null return indicates that someone else holds the lock.
                     */
                    try {
                        
                        raf.close();
                        
                    } catch (Throwable t) {
                        
                        // log and ignore.
                        log.error(t, t);
                        
                    }
    
                    /*
                     * We were not able to get a lock on the file.
                     */
                    throw new RuntimeException("Already locked: "
                            + file.getAbsoluteFile());
    
                }
                
            } catch (IOException ex) {
                
                /*
                 * The platform does not support FileLock (memory mapped files,
                 * read-only files, NFS mounted files all have this problem).
                 */
                log.warn("FileLock not supported: file="
                        + file.getAbsolutePath() + " : " + ex);
    
                return _acquireAdvisoryLock(raf,file);
                
            }
    
        } else {
            
            /*
             * FileLock not requested or explicitly disabled for the JVM.
             */
    
            return _acquireAdvisoryLock(raf, file);
            
        }
    
    }

    private static RandomAccessFile _acquireAdvisoryLock(RandomAccessFile raf,
            File file) throws IOException {

        try {
            
            // seek an advisory lock.
            if (acquireAdvisoryLock(file)) {

                // obtained advisory lock.
                return raf;
            }
            
            // someone else holds the advisory lock.
            try {
                raf.close();
            } catch (IOException t) {
                // log and ignore.
                log.error(t, t);
            }

            throw new IOException("Advisory lock exists: " + file.getAbsolutePath());
            
        } catch (IOException ex2) {

            log.error("Error while seeking advisory lock: file=" + file.getAbsolutePath(), ex2);
            
            try {
                raf.close();
            } catch (IOException t) {
                // log and ignore.
                log.error(t, t);
            }
            throw ex2;
            
        }

    }
    
    /**
     * Close the file and automatically releases the {@link FileLock} (if any)
     * and removes the advisory lock for that file (if any).
     * <p>
     * Note: This method should be used in combination with
     * {@link FileLockUtility#openFile(File, String, boolean)} in order to ensure that the
     * optional advisory lock file is deleted when the file is closed. The
     * purpose of the advisory lock file is to provide advisory locking file
     * modes (read-only), platforms, or file systems (NFS) that do not support
     * {@link FileLock}.
     * 
     * @param file
     *            The file.
     * @param raf
     *            The {@link RandomAccessFile}.
     * 
     * @throws IOException
     */
    public static void closeFile(File file, RandomAccessFile raf)
            throws IOException {

        if (file == null)
            throw new IllegalArgumentException();
        
        if (raf == null)
            throw new IllegalArgumentException();
        
        try {

            if (raf.getChannel().isOpen()) {

                /*
                 * close the file iff open.
                 * 
                 * Note: a thread that is interrupted during an IO can cause the
                 * file to be closed asynchronously. This is handled by the
                 * disk-based store modes.
                 */
                raf.close();

            }

        } finally {

            /*
             * Remove the advisory lock (if present) regardles of whether the
             * file is currently open (see note above).
             */
            removeAdvisoryLock(file);

        }

    }

    /**
     * Creates an advisory lock file having the same basename as the given file
     * with a <code>.lock</code> extension.
     * <p>
     * Note: This uses {@link File#createNewFile()} which is NOT advised for
     * this purpose. However, {@link FileLock} does not work in some contexts so
     * this is used as a fallback mechanism. We write a {@link UUID} into the
     * advisory lock since Java does not have platform independent PIDs. That
     * {@link UUID} allows us to tell whether the advisory lock file was created
     * by this process or by another process.
     * <p>
     * Note: If a {@link Thread} is interrupted during an NIO operation then the
     * {@link FileChannel} will be closed asynchronously. While this correctly
     * releases a {@link FileLock} it does NOT cause our advisory lock file to
     * be deleted. During a normal shutdown of an {@link AbstractJournal}, the
     * advisory lock file is deleted by
     * {@link #closeFile(File, RandomAccessFile)}. However, following an
     * abnormal shutdown the advisory lock file MAY still exist and (assuming
     * that {@link FileLock} is not working since we created an advisory lock in
     * the first place) it MUST be removed by hand before the
     * {@link AbstractJournal} can be reopened.
     * 
     * @param file
     *            The given file.
     * 
     * @return <code>true</code> if the advisory lock was created or exists
     *         and was created by this process. <code>false</code> if the
     *         advisory lock already exists and was created by another process.
     * 
     * @throws IOException
     *             If there is a problem.
     * 
     * @see #pid
     */
    synchronized public static boolean acquireAdvisoryLock(File file)
            throws IOException {

        if (INFO)
            log.info("Seeking advisory lock: " + file.getAbsolutePath());
        
        final File lockFile = new File(file + ".lock");

        if(lockFile.exists()) {
            
            // check the signature in the lock file.
            return isOurLockFile(lockFile);
            
        }
        
        if(!lockFile.createNewFile()) {
            
            // someone else got there first.
            return false;
            
        }
        
        {
            
            final BufferedWriter w = new BufferedWriter(
                    new FileWriter(lockFile));

            try {

                w.write(pid);

                w.write('\n');

                w.flush();
                
            } finally {

                w.close();

            }
            
        }
        
        if (INFO)
            log.info("Created advisory lock: " + file.getAbsolutePath());
        
        return true;

    }

    static public boolean isOurLockFile(File lockFile) throws IOException {

        final BufferedReader r = new BufferedReader(new FileReader(lockFile));

        try {

            final String str = r.readLine();

            if (pid.equals(str))
                return true;

            return false;

        } finally {

            r.close();
            
        }
        
    }
    
    /**
     * Removes the advisory lock for the file if it exists.
     * 
     * @param file
     *            The file whose <code>.lock</code> file will be removed.
     * 
     * @throws IOException
     *             if the lock file exists but does not belong to this process
     *             or can not be removed.
     * 
     * @see #acquireAdvisoryLock(File)
     */
    synchronized public static void removeAdvisoryLock(File file)
            throws IOException {

        final File lockFile = new File(file + ".lock");

        // no advisory lock file.
        if (!lockFile.exists()) return;
        
        if (!isOurLockFile(lockFile)) {
            
            throw new IOException("Not our lock file: " + lockFile.getAbsolutePath());
            
        }

        if (!lockFile.delete()) {

            throw new IOException("Could not delete lock file: " + lockFile.getAbsolutePath());

        }


    }

    /**
     * Since Java does not have platform independent PIDs we use a static
     * {@link UUID} to identify this process. This {@link UUID} gets written
     * into all advisory lock files that the process creates. Another process
     * should check the {@link UUID} in the advisory lock file and refuse to
     * open the file if the {@link UUID} is not its own {@link UUID}.
     */
    static String pid = UUID.randomUUID().toString();
    
}
