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
 * Created on Jul 21, 2009
 */

package com.bigdata.service.ndx.pipeline;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase2;

import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.jini.master.AbstractResourceScanner;
import com.bigdata.service.jini.master.FileSystemScanner;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFileSystemScanner extends TestCase2 {

    /**
     * 
     */
    public TestFileSystemScanner() {
    }

    public TestFileSystemScanner(String a) {
        super(a);
    }
    
    public void test_runScanner() throws Exception {
        
        final BlockingBuffer<File[]> buffer = new BlockingBuffer<File[]>();
        
        final AbstractResourceScanner<File> scanner = FileSystemScanner.newFactory(
                new File("bigdata/src/java/com/bigdata/service/ndx/pipeline"), new FilenameFilter() {

                    public boolean accept(File dir, String name) {
                        System.err.println("Considering: "+dir+File.separator+name);
                        return name.endsWith(".java");
                    }

                }).newScanner(buffer);

        /*
         * Drains buffer, verifying chunks are well formed.
         */
        class DrainBuffer implements Callable<Long> {
            
            public Long call() {
                
                long n = 0L;
                
                final Iterator<File[]> itr = buffer.iterator();
                
                while(itr.hasNext()) {
                    
                    final File[] files = itr.next();
                    
                    assertNotNull(files);
                    assertTrue(files.length!=0);
                    for(File file : files) {
                        assertNotNull(file);
                    }
                    
                    System.err.println(Arrays.toString(files));
                    
                    n += files.length;
                    
                }
                
                return Long.valueOf(n);
            
            }
            
        }
        
        final ExecutorService service = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());
        try {

            final Future<Long> future = service.submit(new DrainBuffer());

            // buffer will be abort()ed if task fails.
            buffer.setFuture(future);
            
            final Long acceptCount = scanner.call();
            
            System.out.println("Scanner accepted: "+acceptCount+" files");

            // close buffer so task draining the buffer will terminate.
            buffer.close();
            
            // compare the accept count with the drain task count.
            assertEquals(acceptCount, future.get());
            
        } finally {
         
            service.shutdownNow();
        
        }
        
    }

}
