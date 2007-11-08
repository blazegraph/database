/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Aug 4, 2006
 */

package com.bigdata.smp;

import java.util.Properties;

/**
 * Utility class designed to verify that multiple processes are in fact being
 * used by Java on a given OS and JVM configuration. You run
 * {@link #main(String[])} specifying the #of threads to create on the command
 * line and then use a command like <code>top</code> or the process monitor
 * and verify that the CPUs on your machine are being pegged. The threads just
 * compute a running sum in a tight loop, so N CPUs should be pegged where N is
 * the #of threads. If you specify more threads than there are CPUs then there
 * may be a decrease in overall efficiency since the threads will need to be
 * swapped to simulate concurrent execution.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSMP {

    /**
     * 
     */
    public TestSMP() {
        super();
    }

    /**
     * Reports properties of interest for the benchmark/test.
     */
    public static void reportProperties() {
        Properties properties = System.getProperties();
        
        for( int i=0; i<report.length; i++ ) {
            System.err.println(report[i]+"="+properties.get(report[i]));
        }        
    }

    /**
     * Properties of interest for the test.
     */
    public static final String[] report = new String[] {
            "java.vm.version",
            "java.vm.vendor",
            "java.vm.info",
            "os.arch",
            "os.name",
            "sun.arch.data.model",
            "sun.os.patch.level",
            "java.class.version"
    };
    

    /**
     * 
     */
    public void doSMPTest( int nthreads ) {
        
        Thread[] threads = new Thread[nthreads];
        
        for( int i=0; i<nthreads; i++ ) {
            
            threads[i] = new TestThread("TestThread["+i+"]");
            
        }
        
        for( int i=0; i<nthreads; i++ ) {
            
            threads[i].start();
            
        }
        
    }
    
    public static class TestThread extends Thread
    {
        
        public TestThread(String name) {
            super(name);
        }

        public void run() {

            System.err.println(currentThread().getName()+": begin.");
            
            long begin = System.currentTimeMillis();
            
            long total = 0L;
            
            for( long i=0; i<10000000000L; i++ ) {
                
                total += i;
                
            }

            long elapsed = System.currentTimeMillis() - begin;
           
            
            System.err.println(currentThread().getName()+": total="+total + " in "+elapsed+" ms");
            
        }
        
    }

    /**
     * 
     * @param args
     * 
     * @todo Report the CPU count (how)?
     */
    public static void main(String[] args) {
        
        reportProperties();

        if( args.length == 0 ) {
            
            System.err.println("You must specify the #of threads to run.");

            System.exit(1);

        }
        
        int nthreads = Integer.parseInt(args[0]);

        System.err.println("nthreads="+nthreads);
        
        if( nthreads <= 1 ) {
            
            System.err.println("#of threads must be >= 1");
            
            System.exit(1);
            
        }
        
        new TestSMP().doSMPTest( nthreads );
        
        // Note: waiting for threads to die.
        
    }
    
}
