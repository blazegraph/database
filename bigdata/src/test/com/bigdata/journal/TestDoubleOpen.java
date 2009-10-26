/**

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Test the ability to rollback a commit.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDoubleOpen extends ProxyTestCase<Journal> {

    /**
     * 
     */
    public TestDoubleOpen() {
    }

    /**
     * @param name
     */
    public TestDoubleOpen(String name) {
     
        super(name);
        
    }

    /**
     * This unit test was written to track down an exception which is not always
     * thrown for this condition. When the exception is thrown, it is thrown on
     * the first double-open request. If the first double-open request succeeds,
     * then the next 99 will also succeed. What we SHOULD be seeing is a nice
     * {@link OverlappingFileLockException}, and that does get thrown a good
     * percentage of the time.
     * 
     * <pre>
     * java.util.concurrent.ExecutionException: java.lang.RuntimeException: file=C:\DOCUME~1\BRYANT~1\LOCALS~1\Temp\bigdata-Disk-6474551553928984593.jnl
     *     at java.util.concurrent.FutureTask$Sync.innerGet(FutureTask.java:222)
     *     at java.util.concurrent.FutureTask.get(FutureTask.java:83)
     *     at com.bigdata.journal.TestDoubleOpen.test_doubleOpen(TestDoubleOpen.java:119)
     *     at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
     *     at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)
     *     at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
     *     at java.lang.reflect.Method.invoke(Method.java:597)
     *     at junit.framework.TestCase.runTest(TestCase.java:154)
     *     at junit.framework.TestCase.runBare(TestCase.java:127)
     *     at junit.framework.TestResult$1.protect(TestResult.java:106)
     *     at junit.framework.TestResult.runProtected(TestResult.java:124)
     *     at junit.framework.TestResult.run(TestResult.java:109)
     *     at junit.framework.TestCase.run(TestCase.java:118)
     *     at junit.framework.TestSuite.runTest(TestSuite.java:208)
     *     at junit.framework.TestSuite.run(TestSuite.java:203)
     *     at org.eclipse.jdt.internal.junit.runner.junit3.JUnit3TestReference.run(JUnit3TestReference.java:130)
     *     at org.eclipse.jdt.internal.junit.runner.TestExecution.run(TestExecution.java:38)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.runTests(RemoteTestRunner.java:467)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.runTests(RemoteTestRunner.java:683)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.run(RemoteTestRunner.java:390)
     *     at org.eclipse.jdt.internal.junit.runner.RemoteTestRunner.main(RemoteTestRunner.java:197)
     * Caused by: java.lang.RuntimeException: file=C:\DOCUME~1\BRYANT~1\LOCALS~1\Temp\bigdata-Disk-6474551553928984593.jnl
     *     at com.bigdata.journal.FileMetadata.<init>(FileMetadata.java:760)
     *     at com.bigdata.journal.AbstractJournal.<init>(AbstractJournal.java:1066)
     *     at com.bigdata.journal.AbstractJournal.<init>(AbstractJournal.java:659)
     *     at com.bigdata.journal.Journal.<init>(Journal.java:136)
     *     at com.bigdata.journal.TestDoubleOpen$1.call(TestDoubleOpen.java:103)
     *     at com.bigdata.journal.TestDoubleOpen$1.call(TestDoubleOpen.java:1)
     *     at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:303)
     *     at java.util.concurrent.FutureTask.run(FutureTask.java:138)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:886)
     *     at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:908)
     *     at java.lang.Thread.run(Thread.java:619)
     * Caused by: java.io.IOException: The handle is invalid
     *     at java.io.RandomAccessFile.length(Native Method)
     *     at com.bigdata.journal.FileMetadata.<init>(FileMetadata.java:420)
     *     ... 10 more
     * </pre>
     * 
     * @todo test read-only for 1st open and read-write for 2nd.
     * 
     * @todo test read-write for first open and read-only for 2nd.
     * 
     * @todo test read-only for both opens after initial create.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void test_doubleOpen() throws InterruptedException, ExecutionException {

        final Journal journal = new Journal(getProperties());

        try {

            if (!journal.isStable()) {

                // test is only for stable journals (backed by a file).
                return;

            }

            final File file = journal.getFile();

            final Properties p = new Properties(journal.getProperties());

            p.setProperty(Journal.Options.CREATE_TEMP_FILE, "false");

            p.setProperty(Journal.Options.FILE, file.toString());

            final int LIMIT = 100;
            
            for (int i = 0; i < LIMIT; i++) {

                if(log.isInfoEnabled()) {
                    
                    log.info("Pass # "+i+" of "+LIMIT);
                    
                }
                
                /*
                 * Try to double-open the journal.
                 */
                
                final Future<Void> future = journal.getExecutorService()
                        .submit(new Callable<Void>() {

                            @Override
                            public Void call() throws Exception {

                                try {

                                    new Journal(p);

                                    fail("Double-open of journal is not allowed");

                                } catch (OverlappingFileLockException ex) {

                                    if (log.isInfoEnabled())
                                        log.info("Double-open refused: " + ex
//                                                ,ex
                                                );

                                }

                                return null;

                            }
                        });

                future.get();

            }

        } finally {

            journal.destroy();

        }

    }
        
}
