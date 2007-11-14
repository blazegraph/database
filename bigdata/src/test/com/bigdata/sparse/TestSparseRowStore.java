/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Aug 13, 2007
 */

package com.bigdata.sparse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.isolation.UnisolatedBTree;
import com.bigdata.journal.TemporaryRawStore;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.DataService;
import com.bigdata.util.CSVReader;

/**
 * Test suite for {@link SparseRowStore}.
 * 
 * @todo test with auto-generated timestamps.
 * 
 * @todo test with application generated timestamps.
 * 
 * @todo test read of historical revision (1st, nth, last).
 * 
 * @todo test history policy (expunging of entries).
 * 
 * @todo verify atomic read/write/scan of rows
 * 
 * @todo specialized compression for the keys using knowledge of schema and
 *       column names?  can we directly produce a compressed representation
 *       that is order preserving?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSparseRowStore extends TestCase2 {

    protected UnicodeKeyBuilder keyBuilder;
    protected IRawStore store;
    protected UnisolatedBTree btree;
    
    public void setUp() {
        
        keyBuilder = new UnicodeKeyBuilder();
        
        store = new TemporaryRawStore();
        
        btree = new UnisolatedBTree(store,UUID.randomUUID());
        
    }
    
    public void tearDown() {
        
        store.closeAndDelete();
        
    }
    
    /**
     * 
     */
    public TestSparseRowStore() {
        super();
    }

    /**
     * @param arg0
     */
    public TestSparseRowStore(String arg0) {
        super(arg0);
    }

    /**
     * Test using a utility class to load CSV data into a {@link SparseRowStore}.
     * 
     * @todo When loading CSV data allow for null vs ""?.
     * 
     * @todo Make this into a utility class.
     * 
     * @todo Allow the caller to specify either a column name that will be the
     *       timestamp or the use of an auto-timestamping mechanism on the
     *       server.
     * 
     * @todo Only store the difference between the last "row" and the current
     *       "row" for a given primary key. This MUST be done on the server in
     *       order for the difference operation to be atomic.
     * 
     * @todo Generalize filters and use on {@link BTree} and {@link DataService}.
     * 
     * @throws IOException
     */
    public void test_loadCSV() throws IOException {

        final String resourceName = "com/bigdata/sparse/test.csv";
        
        final String charSet = "ASCII";
        
        final Schema schema = new Schema("Employee","Id",KeyType.Long);
        
        /*
         * The name of the column that is used as the column value timestamp.
         */
        final String timestampColumn = "DateOfHire";
        
        InputStream is = getTestInputStream(resourceName);

        CSVReader r = new CSVReader(is,charSet);
        
        SparseRowStore srs = new SparseRowStore(btree,keyBuilder,schema);
        
        /*
         * The ground truth data read from the test resource.
         */
        Vector<Map<String,Object>> rows = new Vector<Map<String,Object>>();
        
        /*
         * @todo use mark/reset to avoid having to re-encode the schema and
         * primary key for each column.
         */
        try {
            
            r.readHeaders();
            
            while(r.hasNext()) {

                /*
                 * Use a timestamp factory to give each record a unique
                 * timestamp.
                 */
//                long timestamp = TimestampFactory.nextNanoTime();
                
                // read a row of data.
                Map<String,Object> row = r.next();

                // remember for validation below.
                rows.add(row);
                
                /*
                 * Use the date of hire column as the timestamp on the record.
                 */
                long timestamp = ((Date)row.get(timestampColumn)).getTime();
                
                /*
                 * FIXME compute the difference from the current row and store
                 * only the difference -- this should perhaps be done inside of
                 * write(). without this step, each row loaded replicates all
                 * column values.
                 */
                
                // write on the sparse row store.
                srs.write(row, timestamp );

                /*
                 * Verify read back of the row that we just wrote.
                 */
                {

                    // extract the primary key for this row.
                    Object primaryKey = row.get(srs.getSchema().getPrimaryKey());

                    // read the row from the store.
                    Map<String, Object> actual = srs.read(primaryKey, timestamp);

                    assertNotNull("No such row: " + primaryKey, actual);

                    assertSameValues(row, actual);

                }

            }
            
        } finally {
            
            is.close();
            
        }

        /*
         * Dump the data in the index.
         *
         * @todo make this a utility method on the sparse row store if I can solve
         * the general problem of locating the column name.
         */
        {
            
            IEntryIterator itr = btree.entryIterator();

            /*
             * @todo this is tricky and hardcoded. we lack a general solution to
             * locate the column name in the key.
             */
            final int offsetColumnName = schema.getSchemaBytesLength()
                    + Bytes.SIZEOF_LONG; 
            
            while(itr.hasNext()) {
            
                final byte[] val = (byte[])itr.next();
                
                final byte[] key = itr.getKey();

                KeyDecoder keyDecoder = new KeyDecoder(schema,key,offsetColumnName);
                
                System.err.println(keyDecoder.col + "=" + ValueType.decode(val)
                        + " (" + keyDecoder.timestamp + ")");
                
            }
            
        }
        
        /*
         * Verify read back of all rows against the ground truth data.
         * 
         * Note: When the test data contains multiple rows with different
         * timestamps for the same primary key we need to pass in the timestamp
         * in order to recover the desired revision.
         */
        {
            
            final int nrows = rows.size();
            
            for( int i=0; i<nrows; i++) {
               
                System.err.println("Verifying row# "+(i+1));
                
                Map<String,Object> expected = rows.get(i);
                
                // extract the primary key for this row.
                Object primaryKey = expected.get(srs.getSchema().getPrimaryKey());
                
                /*
                 * Use the date of hire column as the timestamp on the record.
                 */
                long timestamp = ((Date)expected.get(timestampColumn)).getTime();

                // read the row from the store.
                Map<String,Object> actual = srs.read(primaryKey, timestamp);
                
                assertNotNull("No such row: "+primaryKey,actual);
                
                assertSameValues(expected,actual);
                
            }
            
        }
        
        /*
         * @todo verify scan of rows.
         * 
         */

        
    }

    /**
     * Test that a read for a row that does not exist returns <code>null</code>.
     */
    public void test_read_noSuchRow() {
        
        final Schema schema = new Schema("Employee","Id",KeyType.Long);
        
        SparseRowStore srs = new SparseRowStore(btree,keyBuilder,schema);
        
        assertNull(srs.read(Long.valueOf(0L)));
        
    }
    
    /**
     * Verify that two rows have the same column values.
     */
    protected void assertSameValues(Map<String,Object> expected, Map<String,Object> actual) {
        
        assertEquals("#of values", expected.size(), actual.size() );
        
        Iterator<String> itr = expected.keySet().iterator();

        while(itr.hasNext()) {
            
            String col = itr.next();
            
            assertTrue("No value: col=" + col, actual.containsKey(col));
            
            Object expectedValue = expected.get(col);

            Object actualValue = actual.get(col);
            
            assertNotNull(col+" is null.", actualValue);
            
            assertEquals(col, expectedValue.getClass(), actualValue.getClass());
            
            assertEquals(col, expectedValue, actualValue);
            
        }
        
    }

//    /**
//     * Tests for keys formed from the application key, a column name, and a long
//     * timestamp. A zero(0) byte is used as a delimiter between components of
//     * the key.
//     * 
//     * @todo this is not testing much yet and should be in its own test suite.
//     */
//    public void test_cstore_keys() {
//        
//        IKeyBuilder keyBuilder = new UnicodeKeyBuilder();
//        
//        final byte[] colname1 = keyBuilder.reset().append("column1").getKey();
//        
//        final byte[] colname2 = keyBuilder.reset().append("another column").getKey();
//        
//        final long timestamp = System.currentTimeMillis();
//        
//        byte[] k1 = keyBuilder.reset().append(12L).appendNul().append(colname1)
//        .appendNul().append(timestamp).getKey();
//
//        byte[] k2 = keyBuilder.reset().append(12L).appendNul().append(colname2)
//        .appendNul().append(timestamp).getKey();
//
//        System.err.println("k1="+BytesUtil.toString(k1));
//        System.err.println("k2="+BytesUtil.toString(k2));
//
//        fail("this does not test anything yet");
//    }

}
