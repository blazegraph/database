/*

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
/*
 * Created on May 9, 2008
 */

package com.bigdata.counters.osx;

import com.bigdata.counters.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Test suite for parsing the output of the <code>vm_stat</code> utility under
 * OSX.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestParse_vm_stat extends AbstractParserTestCase {

    public TestParse_vm_stat() {
        super();
    }

    public TestParse_vm_stat(String name) {
        super(name);
    }
    
    /**
     * Test based on some sample data.
     */
    public void test_vmstat_header_and_data_parse() {

        final Pattern pattern = VMStatCollector.pattern;

		// first header line.
		final String h0 = "Mach Virtual Memory Statistics: (page size of 4096 bytes, cache hits 0%)";
		// second header line.
		final String h1 = "  free active   spec inactive   wire   faults     copy    0fill reactive  pageins  pageout";
		// data line
		final String d1 = "404670  1238K  42678   161330 183963  144830K  2032186 81929280  1353888   351293   149940";

        // test header parse.
        {
            final String[] fields = pattern.split(h0.trim(), 0/* limit */);

            for (int i = 0; i < fields.length; i++) {

                if(log.isInfoEnabled())
                		log.info("fields[" + i + "]=[" + fields[i] + "]");
                
            }
            
            assertField(h0, fields, 0, "Mach"); // marker
            assertField(h0, fields, 7, "4096"); // page size
            
        }
        {
        	final String[] fields = pattern.split(h1.trim(), 0/* limit */);

            for (int i = 0; i < fields.length; i++) {

                if(log.isInfoEnabled())
                		log.info("fields[" + i + "]=[" + fields[i] + "]");
                
            }
            
            assertField(h1, fields, 0, "free");
            assertField(h1, fields, 1, "active");
            assertField(h1, fields, 2, "spec");
            assertField(h1, fields, 3, "inactive");
            assertField(h1, fields, 4, "wire");
            assertField(h1, fields, 5, "faults");
            assertField(h1, fields, 6, "copy");
            assertField(h1, fields, 7, "0fill");
            assertField(h1, fields, 8, "reactive");
            assertField(h1, fields, 9, "pageins");
            assertField(h1, fields,10, "pageout");

		}

		// test data parse.
//  	final String h1 = "  free active   spec inactive   wire   faults     copy    0fill reactive  pageins  pageout";
//		final String d1 = "404670  1238K  42678   161330 183963  144830K  2032186 81929280  1353888   351293   149940";
		{

			final String[] fields = pattern.split(d1.trim(), 0/* limit */);

			assertField(d1, fields, 0, "404670");  // free
			assertField(d1, fields, 1, "1238K");   // active
			assertField(d1, fields, 2, "42678");   // spec
			assertField(d1, fields, 3, "161330");  // inactive
			assertField(d1, fields, 4, "183963");  // wire
			assertField(d1, fields, 5, "144830K"); // faults
			assertField(d1, fields, 6, "2032186"); // copy
			assertField(d1, fields, 7, "81929280");// 0fill
			assertField(d1, fields, 8, "1353888"); // reactive
			assertField(d1, fields, 9, "351293");  // pageins
			assertField(d1, fields,10, "149940");  // pageout
		}

    }

    /**
     * <pre>
Mach Virtual Memory Statistics: (page size of 4096 bytes)
    free   active   specul inactive throttle    wired  prgable   faults     copy    0fill reactive   purged file-backed anonymous cmprssed cmprssor  dcomprs   comprs  pageins  pageout  swapins swapouts
   17656  1502912   110218  1329298        0   783121    32302 1122834K 22138464  897721K  4538117  2220029      382716   2559712  1093181   449012  1856773  4298532 67248050   263644   779970   886296
   41993  1479331   110174  1329290        0   782241    33940     2115        1     1316        0        0      382671   2536124  1093181   449012        0        0       24        0        0        0
     * </pre>
     * 
     * @see #1125 (OSX vm_stat output has changed)
     */
    public void test_parse_osx_yosemite() {

       final Pattern pattern = VMStatCollector.pattern;

     // first header line.
     final String h0 = "Mach Virtual Memory Statistics: (page size of 4096 bytes)";
     // second header line.
     final String h1 = "    free   active   specul inactive throttle    wired  prgable   faults     copy    0fill reactive   purged file-backed anonymous cmprssed cmprssor  dcomprs   comprs  pageins  pageout  swapins swapouts";
     // data line
     final String d1 = "   41993  1479331   110174  1329290        0   782241    33940     2115        1     1316        0        0      382671   2536124  1093181   449012        0        0       24        0        0        0";

       // test header parse.
       {
           final String[] fields = pattern.split(h0.trim(), 0/* limit */);

           for (int i = 0; i < fields.length; i++) {

               if(log.isInfoEnabled())
                    log.info("fields[" + i + "]=[" + fields[i] + "]");
               
           }
           
           assertField(h0, fields, 0, "Mach"); // marker
           assertField(h0, fields, 7, "4096"); // page size
           
       }
       {
        final String[] fields = pattern.split(h1.trim(), 0/* limit */);

           for (int i = 0; i < fields.length; i++) {

               if(log.isInfoEnabled())
                    log.info("fields[" + i + "]=[" + fields[i] + "]");
               
           }
           
           assertField(h1, fields, 0, "free");
           assertField(h1, fields, 1, "active");
           assertField(h1, fields, 2, "specul");
           assertField(h1, fields, 3, "inactive");
           assertField(h1, fields, 4, "throttle");
           assertField(h1, fields, 5, "wired");
           assertField(h1, fields, 6, "prgable");
           assertField(h1, fields, 7, "faults");
           assertField(h1, fields, 8, "copy");
           assertField(h1, fields, 9, "0fill");
           assertField(h1, fields,10, "reactive");
           assertField(h1, fields,11, "purged");
           assertField(h1, fields,12, "file-backed");
           assertField(h1, fields,13, "anonymous");
           assertField(h1, fields,14, "cmprssed");
           assertField(h1, fields,15, "cmprssor");
           assertField(h1, fields,16, "dcomprs");
           assertField(h1, fields,17, "comprs");
           assertField(h1, fields,18, "pageins");
           assertField(h1, fields,19, "pageout");
           assertField(h1, fields,20, "swapins");
           assertField(h1, fields,21, "swapouts");

     }

     // test data parse.
//     final String h1 = "    free   active   specul inactive throttle    wired  prgable   faults     copy    0fill reactive   purged file-backed anonymous cmprssed cmprssor  dcomprs   comprs  pageins  pageout  swapins swapouts";
//     final String d1 = "   41993  1479331   110174  1329290        0   782241    33940     2115        1     1316        0        0      382671   2536124  1093181   449012        0        0       24        0        0        0";
     {

        final String[] fields = pattern.split(d1.trim(), 0/* limit */);

        assertField(d1, fields, 0, "41993");  // free
        assertField(d1, fields, 1, "1479331");   // active
        assertField(d1, fields, 2, "110174");   // specul
        assertField(d1, fields, 3, "1329290");  // inactive
        assertField(d1, fields, 4, "0");  // throttle
        assertField(d1, fields, 5, "782241");  // wired
        assertField(d1, fields, 6, "33940");  // prgable
        assertField(d1, fields, 7, "2115"); // faults
        assertField(d1, fields, 8, "1"); // copy
        assertField(d1, fields, 9, "1316");// 0fill
        assertField(d1, fields,10, "0"); // reactive
        assertField(d1, fields,11, "0"); // purged
        assertField(d1, fields,12, "382671"); // file-backed
        assertField(d1, fields,13, "2536124"); // anonymous
        assertField(d1, fields,14, "1093181"); // cmprssed
        assertField(d1, fields,15, "449012"); // cmprssor
        assertField(d1, fields,16, "0"); // dcomprs
        assertField(d1, fields,17, "0"); // comprs
        assertField(d1, fields,18, "24");  // pageins
        assertField(d1, fields,19, "0");  // pageout
        assertField(d1, fields,20, "0"); // swapins
        assertField(d1, fields,21, "0"); // swapouts

     }
       
    }


    public void test_iostat_collector() throws IOException, InterruptedException {

        String output =
        "Mach Virtual Memory Statistics: (page size of 4096 bytes, cache hits 0%)\n" +
        "  free active   spec inactive   wire   faults     copy    0fill reactive  pageins  pageout\n" +
        "404670  1238K  42678   161330 183963  144830K  2032186 81929280  1353888   351293   149940\n";

        final VMStatCollector vmStatCollector = new VMStatCollector(1) {
            @Override
            public AbstractProcessReader getProcessReader() {
                return new VMStatReader() {
                    @Override
                    protected ActiveProcess getActiveProcess() {
                        return new ActiveProcess() {
                            @Override
                            public boolean isAlive() {
                                return true;
                            }
                        };
                    }
                };
            }
        };
        final VMStatCollector.VMStatReader vmStatReader = (VMStatCollector.VMStatReader) vmStatCollector.getProcessReader();
        vmStatReader.start(new ByteArrayInputStream(output.getBytes()));
        Thread t = new Thread(vmStatReader);
        CounterSet counterSet;
        try {
            t.start();
            Thread.sleep(100);
            counterSet = vmStatCollector.getCounters();
        } finally {
            t.interrupt();
        }

        double bytes_free = (Double) ((ICounter) counterSet.getChild(IProcessCounters.Memory).getChild("Bytes Free")).getInstrument().getValue();
        assertEquals(404670*4096, bytes_free);



        /*Iterator<ICounter> counters = counterSet.getCounters(Pattern.compile(".*"));
        while (counters.hasNext()) {
            System.err.println(counters.next().getInstrument().getValue().toString());
        }*/

    }
    
}
