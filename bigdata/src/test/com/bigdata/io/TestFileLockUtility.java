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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import junit.framework.TestCase2;

/**
 * Test suite for {@link FileLockUtility}.
 * 
 * @todo this test suite could be developed further.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFileLockUtility extends TestCase2 {

    /**
     * 
     */
    public TestFileLockUtility() {

    }

    /**
     * @param arg0
     */
    public TestFileLockUtility(String arg0) {

        super(arg0);
        
    }

    /**
     * Test verifies creation of an exclusive lock, that an advisory lock was
     * NOT created, and that the {@link FileChannel} is correctly closed.
     * 
     * @throws IOException
     */
    public void test_exclusiveLock() throws IOException {
        
        final File file = File.createTempFile(getClass().getSimpleName(),
                getName());

        RandomAccessFile raf = null;
        
        try {
            
            raf = FileLockUtility.openFile(file, "rw", true/* useFileLock */);

            final File lockFile = new File(file + ".lock");

            assertFalse("Created an advisory lock: file=" + file, lockFile
                    .exists());

            try {

                // verify FileLock is in place.
                raf.getChannel().tryLock();

                fail("Expecting: " + OverlappingFileLockException.class);

            } catch (OverlappingFileLockException ex) {

                log.info("Ignoring expected exception: " + ex);

            }

            // close the file.
            FileLockUtility.closeFile(file, raf);

            // verify that the channel was closed.
            raf.getChannel().isOpen();
            
        } finally {

            if (raf != null && raf.getChannel().isOpen()) {

                raf.close();
                
            }

            file.delete();
            
        }

    }

    /**
     * Test creation of an advisory lock, that
     * {@link FileLockUtility#openFile(File, String, boolean)} will not grant an
     * exclusive {@link FileLock} if an advisory lock exists for a file, and
     * that the advisory lock is removed when the file is closed using
     * {@link FileLockUtility#closeFile(File, RandomAccessFile)}.
     * 
     * @throws IOException
     */
    public void test_advisoryLock() throws IOException {
        
        final File file = File.createTempFile(getClass().getSimpleName(),
                getName());

        final File lockFile = new File(file + ".lock");

        RandomAccessFile raf = null;
        
        try {
            
            raf = FileLockUtility.openFile(file, "rw", false/* useFileLock */);

            assertTrue("Did not create an advisory lock: file=" + file, lockFile
                    .exists());

            try {

                // verify FileLock is NOT in place.
                FileLock fileLock = raf.getChannel().tryLock();

                assertNotNull("FileLock was created.", fileLock);

            } catch (Throwable t) {

                log.warn("FileLock not available: " + t + " for " + file);

            }
            
            /*
             * In fact, we allow "re-entrant" advisory locks in order to handle
             * asynchronous close of the file channel by an interrupt.
             * Asynchronous close correctly releases a FileLock, but there is no
             * way to have it delete our advisory lock so we allow re-entrant
             * advisory locks instead as long as it is the same process seeking
             * the lock.
             */
            
//            try {
//                
//                /*
//                 * verify that we notice an existing advisory lock when
//                 * requsting another advisory lock for the same file.
//                 */
//                FileLockUtility.openFile(file, "rw", false/*useFileLock*/);
//                
//                fail("Expecting: "+IOException.class);
//                
//            } catch(IOException ex) {
//                
//                log.warn("Ignoring expected exception: "+ex);
//                
//            }

            try {
                
                /*
                 * verify that we notice an existing advisory lock when
                 * requsting an exclusive lock for the same file.
                 */
                FileLockUtility.openFile(file, "rw", true/* useFileLock */);
                
                fail("Expecting: "+IOException.class);
                
            } catch(IOException ex) {
                
                log.warn("Ignoring expected exception: "+ex);
                
            }

            // close the file.
            FileLockUtility.closeFile(file, raf);

            // verify that the channel was closed.
            raf.getChannel().isOpen();
            
            // verify that the advisory lock was removed.
            assertFalse("Did not remove advisory lock: file=" + file, lockFile
                    .exists());
            
        } finally {

            if (raf != null && raf.getChannel().isOpen()) {

                raf.close();
                
            }

            file.delete();

            lockFile.delete();
            
        }

    }
    
}
