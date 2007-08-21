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
 * Created on Aug 13, 2007
 */

package com.bigdata.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase2;

import com.bigdata.util.CSVReader.Header;

/**
 * Test suite for {@link CSVReader}.
 * 
 * @todo test "correct" default intepretation of more kinds of formats by
 *       {@link Header#parseValue(String)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCSVReader extends TestCase2 {

    /**
     * 
     */
    public TestCSVReader() {
        super();
    }

    /**
     * @param name
     */
    public TestCSVReader(String name) {
        super(name);
    }

    public void test_ctor_correctRejection() throws IOException {
        
        try {
            new CSVReader(null,"UTF-8");
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            log.info("Ignoring expected exception: "+ex);
        }

        try {
            new CSVReader(new ByteArrayInputStream(new byte[]{}),null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            log.info("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * Test reads from a tab-delimited file <code>test.csv</code> with headers
     * in the first row and two rows of data.
     * 
     * @throws IOException
     * @throws ParseException
     */
    public void test_read_test_csv() throws IOException, ParseException {
        
        Header[] headers = new Header[] {
          
                new Header("Name"),
                new Header("Id"),
                new Header("Employer"),
                new Header("DateOfHire"),
                new Header("Salary"),
                
        };
        
        CSVReader r = new CSVReader(
                getTestInputStream("com/bigdata/util/test.csv"), "UTF-8");

        /* 
         * read headers.
         */
        assertTrue(r.hasNext());

        r.readHeaders();
        
        assertEquals(1,r.lineNo());

        assertEquals(headers, r.headers);
        
        /*
         * 1st row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues(newMap(headers, new Object[] { "Bryan Thompson",
                new Long(12), "SAIC",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2002"),
                new Double(12.02)
        }), r.next() );

        assertEquals(2,r.lineNo());

        
        /*
         * 2nd row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues( newMap(headers, new Object[]{
                "Bryan Thompson",
                new Long(12), "SYSTAP",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2005"),
                new Double(13.03)
        }), r.next() );

        assertEquals(3,r.lineNo());

        /* 
         * Verify EOF.
         */
        assertFalse(r.hasNext());
        
    }

    /**
     * Test reads from a tab-delimited file <code>test-no-headers.csv</code>
     * with NO headers and two rows of data.
     * 
     * @throws IOException
     * @throws ParseException
     */
    public void test_read_test_no_headers_csv() throws IOException, ParseException {
        
        Header[] headers = new Header[] {
          
                new Header("1"),
                new Header("2"),
                new Header("3"),
                new Header("4"),
                new Header("5"),
                
        };
        
        CSVReader r = new CSVReader(
                getTestInputStream("com/bigdata/util/test-no-headers.csv"), "UTF-8");

        /*
         * 1st row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues(newMap(headers, new Object[] { "Bryan Thompson",
                new Long(12), "SAIC",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2002"),
                new Double(12.02)
        }), r.next() );

        assertEquals(1,r.lineNo());

        
        /*
         * 2nd row of data.
         */
        assertTrue(r.hasNext());

        assertSameValues( newMap(headers, new Object[]{
                "Bryan Thompson",
                new Long(12), "SYSTAP",
                new SimpleDateFormat("MM/dd/yy").parse("4/30/2005"),
                new Double(13.03)
        }), r.next() );

        assertEquals(2,r.lineNo());

        /* 
         * Verify EOF.
         */
        assertFalse(r.hasNext());
        
    }

    protected void assertEquals(Header[] expected, Header[] actual) {
        
        assertEquals(expected.length,actual.length);
        
        for(int i=0; i<expected.length; i++) {
            
            if(!expected[i].equals( actual[i])) {
                
                fail("headers["+i+"], expected ["+expected[i]+"]u not ["+actual[i]+"]" );
                
            }
            
        }
        
    }
    
    /**
     * Form data structure modeling an expected (parsed) row.
     * 
     * @param headers
     *            The headers.
     * @param vals
     *            The values (one per header and in the same order).
     *            
     * @return The map containing the appropriate values.
     */
    protected Map<String,Object> newMap(Header[] headers, Object[] vals) {

        assert headers.length==vals.length;
        
        Map<String,Object> map = new TreeMap<String,Object>();
        
        for(int i=0; i<headers.length; i++) {
            
            map.put(headers[i].name,vals[i]);
            
        }
        
        assert map.size() == headers.length;
        
        return map;
        
    }
    
    protected void assertSameValues(Map<String,Object> expected, Map<String,Object> actual) {
        
        assertEquals("#of values", expected.size(), actual.size() );
        
        Iterator<String> itr = expected.keySet().iterator();

        while(itr.hasNext()) {
            
            String col = itr.next();
            
            assertTrue("No value: col=" + col, actual.containsKey(col));
            
            Object expectedValue = expected.get(col);

            Object actualValue = actual.get(col);
            
            assertNotNull("Col="+col+" is null.", actualValue);
            
            assertEquals("Col="+col, expectedValue.getClass(), actualValue.getClass());
            
            assertEquals("Col="+col, expectedValue, actualValue);
            
        }
        
    }
    
}
