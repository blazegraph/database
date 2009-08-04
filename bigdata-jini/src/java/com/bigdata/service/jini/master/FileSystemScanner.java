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
 * Created on Jul 11, 2009
 */

package com.bigdata.service.jini.master;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Queue;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.BlockingBuffer;

/**
 * Adds all files accepted by the filter to the {@link Queue}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileSystemScanner extends AbstractResourceScanner<File> {

    volatile boolean done = false;

    protected final File fileOrDir;

    protected final FilenameFilter filter;

    /**
     * @param buffer
     *            Chunks of files to be loaded are added to this buffer.
     * @param fileOrDir
     *            The file or directory to be loaded.
     * @param filter
     *            An optional filter on files that will be accepted when
     *            processing a directory.
     */
    public FileSystemScanner(final BlockingBuffer<File[]> buffer,
            final File fileOrDir, final FilenameFilter filter) {

        super(buffer);

        if (fileOrDir == null)
            throw new IllegalArgumentException();

        this.fileOrDir = fileOrDir;

        this.filter = filter; // MAY be null.

    }

    @Override
    protected void runScanner() throws Exception {
        
        process2(fileOrDir);
        
    }
    
    /**
     * Scans file(s) recursively starting with the named file, and, for each
     * file that passes the filter, submits the task.
     * 
     * @param file
     *            Either a URL, a plain file or directory containing files
     *            to be processed.
     * 
     * @throws InterruptedException
     *             if the thread is interrupted while queuing tasks.
     */
    private void process2(final File file) throws InterruptedException {

        if (file.isHidden()) {

            // ignore hidden files.
            return;

        }

        if (file.isDirectory()) {

            if (log.isInfoEnabled())
                log.info("Scanning directory: " + file);

            // filter is optional.
            final File[] files = filter == null ? file.listFiles() : file
                    .listFiles(filter);

            for (final File f : files) {

                process2(f);

            }

        } else {

            /*
             * Processing a standard file.
             */

            accept(file);

        }

    }

    /**
     * Factory for factory.
     */
    public static IResourceScannerFactory<File> newFactory(
            final File fileOrDir, final FilenameFilter filter) {

        return new IResourceScannerFactory<File>() {

            /**
             * 
             */
            private static final long serialVersionUID = 6440345409026346627L;

            public AbstractResourceScanner<File> newScanner(
                    final BlockingBuffer<File[]> buffer) {
                
                return new FileSystemScanner(buffer, fileOrDir, filter);
                
            }
            
        };
        
    }

}
