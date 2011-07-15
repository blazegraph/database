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
 * Created on May 9, 2008
 */

package com.bigdata.counters.osx;

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

}
