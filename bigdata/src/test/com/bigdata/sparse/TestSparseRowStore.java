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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import junit.framework.TestCase2;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.repo.BigdataRepository;
import com.bigdata.util.CSVReader;

/**
 * Test suite for {@link SparseRowStore}.
 * <p>
 * Note: A lot of the pragmatic tests are being done by the
 * {@link BigdataRepository} which uses the {@link SparseRowStore} for its file
 * metadata index.
 * 
 * FIXME clean up this test suite.
 * 
 * @todo test with auto-generated timestamps.
 * 
 * @todo test with application generated timestamps.
 * 
 * @todo test read of historical revision (1st, nth, last).
 * 
 * @todo test history policy (expunging of entries).
 * 
 * @todo specialized compression for the keys using knowledge of schema and
 *       column names? can we directly produce a compressed representation that
 *       is order preserving?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSparseRowStore extends TestCase2 {

    protected IKeyBuilder keyBuilder;
    protected IRawStore store;
    protected BTree btree;
    
    public Properties getProperties() {
        
        Properties properties = new Properties(super.getProperties());
        
        properties.setProperty(Options.BUFFER_MODE, "" + BufferMode.Transient);
        
        return properties;
        
    }
    
    public void setUp() {
        
        Properties properties = getProperties();

        keyBuilder = KeyBuilder.newUnicodeInstance(properties);

        store = new Journal(properties);

        IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());

        metadata.setDeleteMarkers(true);

        btree = BTree.create(store, metadata);
        
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
     * @todo Only store the difference between the last "row" and the current
     *       "row" for a given primary key. This MUST be done on the server in
     *       order for the difference operation to be atomic.
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
        
        SparseRowStore srs = new SparseRowStore(btree);
        
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
                 * column values (this is only an issue for things like the CSV
                 * load and could be performed on the client to minimize IO; if
                 * performed on the server then an overwrite of a property value
                 * would not update the timestamp, which might not be
                 * desirable).
                 */
                
                // write on the sparse row store.
                srs.write(schema, row, timestamp, null/*filter*/, null/*precondition*/ );

                /*
                 * Verify read back of the row that we just wrote.
                 */
                {

                    // extract the primary key for this row.
                    Object primaryKey = row.get(schema.getPrimaryKeyName());

                    // read the row from the store.
                    ITPS tps = srs
                            .read(schema, primaryKey, timestamp, null/* filter */);

                    assertNotNull("No such row: " + primaryKey, tps);

                    assertSameValues(row, tps.asMap());

                }

            }
            
        } finally {
            
            is.close();
            
        }

        /*
         * Dump the data in the index.
         */
        {
            
            ITupleIterator itr = btree.rangeIterator();
            
            while(itr.hasNext()) {
            
                ITuple tuple = itr.next();
                
                final byte[] val = tuple.getValue();
                
                final byte[] key = tuple.getKey();

                KeyDecoder keyDecoder = new KeyDecoder(key);
                
                log.info(keyDecoder.getColumnName() + "="
                        + ValueType.decode(val) + " (" + keyDecoder.timestamp
                        + ")");
                
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
                Object primaryKey = expected.get(schema.getPrimaryKeyName());
                
                /*
                 * Use the date of hire column as the timestamp on the record.
                 */
                long timestamp = ((Date)expected.get(timestampColumn)).getTime();

                // read the row from the store.
                ITPS tps = srs
                        .read(schema, primaryKey, timestamp, null/* filter */);
                
                assertNotNull("No such row: "+primaryKey,tps);
                
                assertSameValues(expected,tps.asMap());
                
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
        
        SparseRowStore srs = new SparseRowStore(btree);
        
        assertNull(srs.read(schema,Long.valueOf(0L)));
        
    }

    /**
     * Simple test of write and read back of logical rows.
     */
    public void test_readWrite() {
         
        final Schema schema = new Schema("Employee", "Id", KeyType.Long);

        SparseRowStore srs = new SparseRowStore(btree);

        {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Bryan");
            propertySet.put("State", "NC");

            srs.write(schema, propertySet);

        }

        {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 2L);
            propertySet.put("Name", "Mike");
            propertySet.put("State", "UT");

            srs.write(schema, propertySet);

        }

        {
            
            final Map<String,Object> row = srs.read(schema, 1L);

            assertTrue( row != null );
            
            assertEquals( 1L, row.get("Id") );
            assertEquals( "Bryan", row.get("Name"));
            assertEquals( "NC", row.get("State"));
            
        }
        
        {
            
            final Map<String,Object> row = srs.read(schema, 2L);
         
            assertTrue( row != null );
            
            assertEquals( 2L, row.get("Id") );
            assertEquals( "Mike", row.get("Name"));
            assertEquals( "UT", row.get("State"));
            
        }
        
    }
    
    /**
     * Test of {@link IPrecondition} handling for atomic writes.
     */
    public void test_writeWithPrecondition() {
    
        final Schema schema = new Schema("Employee", "Id", KeyType.Long);

        SparseRowStore srs = new SparseRowStore(btree);

        {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Bryan");
            propertySet.put("State", "DC");

            final TPS tps = srs.write(schema, propertySet, 1L/* timestamp */,
                    null/* filter */, new EmptyRowPrecondition());

            assertTrue(tps.isPreconditionOk());
            
        }

        {
            /*
             * Atomic write where the pre-condition is not satisfied.
             */

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Mike");
            propertySet.put("State", "UT");

            final IPrecondition precondition = new IPrecondition() {

                private static final long serialVersionUID = 1L;

                public boolean accept(ITPS logicalRow) {

                    return "Mike".equals(logicalRow.get("Name").getValue());
                    
                }
                
            };

            final TPS tps = srs.write(schema, propertySet, 1L/* timestamp */,
                    null/* filter */, precondition);

            assertFalse(tps.isPreconditionOk());
            
            // verify row state is unchanged.
            assertEquals(1L,tps.get("Id").getValue());
            assertEquals("Bryan",tps.get("Name").getValue());
            assertEquals("DC",tps.get("State").getValue());
            
        }

        {
            /*
             * Atomic write where the pre-condition is satisfied.
             */

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Bryan");
            propertySet.put("State", "NC");

            final IPrecondition precondition = new IPrecondition() {

                private static final long serialVersionUID = 1L;

                public boolean accept(ITPS logicalRow) {

                    return "Bryan".equals(logicalRow.get("Name").getValue())
                            && "DC".equals(logicalRow.get("State").getValue());

                }

            };

            final TPS tps = srs.write(schema, propertySet, 1L/* timestamp */,
                    null/* filter */, precondition);

            assertTrue(tps.isPreconditionOk());

            // verify row state is changed.
            assertEquals(1L, tps.get("Id").getValue());
            assertEquals("Bryan", tps.get("Name").getValue());
            assertEquals("NC", tps.get("State").getValue());

        }

    }

    /**
     * Test of a logical row scan.
     * 
     * @todo Write a variant in which we test with multiple index partitions to
     *       verify that the {@link AtomicRowScan} is correctly mapped across
     *       the various index partitions.
     */
    public void test_rowScan() {
     
        final Schema schema = new Schema("Employee","Id",KeyType.Long);
        
        SparseRowStore srs = new SparseRowStore(btree);
        
        {
            
            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Bryan");
            propertySet.put("State", "NC");

            srs.write(schema, propertySet);

        }

        {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 2L);
            propertySet.put("Name", "Mike");
            propertySet.put("State", "UT");

            srs.write(schema, propertySet);

        }

        Iterator<? extends ITPS> itr = srs.rangeQuery(schema,
                null/* fromKey */, null/* toKey */);
        
        {
            
            assertTrue(itr.hasNext());

            final ITPS tps = itr.next();
         
            assertTrue( tps != null );
            
            assertEquals(1L, tps.get("Id").getValue());
            assertEquals("Bryan", tps.get("Name").getValue());
            assertEquals("NC", tps.get("State").getValue());
            
        }
        
        {
            
            assertTrue(itr.hasNext());

            final ITPS tps = itr.next();
         
            assertTrue( tps != null );
            
            assertEquals(2L, tps.get("Id").getValue());
            assertEquals("Mike", tps.get("Name").getValue());
            assertEquals("UT", tps.get("State").getValue());
            
        }

        assertFalse(itr.hasNext());
        
    }
    
    /**
     * Test of a logical row scan using a key range limit.
     */
    public void test_rowScan_withKeyRange() {
     
        final Schema schema = new Schema("Employee","Id",KeyType.Long);
        
        SparseRowStore srs = new SparseRowStore(btree);
        
        {
            
            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Bryan");
            propertySet.put("State", "NC");

            srs.write(schema, propertySet);

        }

        {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 2L);
            propertySet.put("Name", "Mike");
            propertySet.put("State", "UT");

            srs.write(schema, propertySet);

        }

        {
            
            Iterator<? extends ITPS> itr = srs.rangeQuery(schema,
                    1L, 2L);
            
            assertTrue(itr.hasNext());

            final ITPS tps = itr.next();
         
            assertTrue( tps != null );
            
            assertEquals(1L, tps.get("Id").getValue());
            assertEquals("Bryan", tps.get("Name").getValue());
            assertEquals("NC", tps.get("State").getValue());

            assertFalse(itr.hasNext());
            
        }
        
        {

            Iterator<? extends ITPS> itr = srs.rangeQuery(schema,
                    2L, null/* toKey */);
            
            assertTrue(itr.hasNext());

            final ITPS tps = itr.next();
         
            assertTrue( tps != null );
            
            assertEquals(2L, tps.get("Id").getValue());
            assertEquals("Mike", tps.get("Name").getValue());
            assertEquals("UT", tps.get("State").getValue());

            assertFalse(itr.hasNext());
            
        }
    
    }
    
    /**
     * Test of a logical row scan requiring continuation queries by forcing the
     * capacity to 1 when there are in fact two logical rows.
     * 
     * FIXME This test fails for a known reason - continuation query semantics
     * have not been implemented yet. See {@link AtomicRowScan}.
     */
    public void test_rowScan_continuationQuery() {
     
        final Schema schema = new Schema("Employee","Id",KeyType.Long);
        
        SparseRowStore srs = new SparseRowStore(btree);
        
        {
            
            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 1L);
            propertySet.put("Name", "Bryan");
            propertySet.put("State", "NC");

            srs.write(schema, propertySet);

        }

        {

            final Map<String, Object> propertySet = new HashMap<String, Object>();

            propertySet.put("Id", 2L);
            propertySet.put("Name", "Mike");
            propertySet.put("State", "UT");

            srs.write(schema, propertySet);

        }

        Iterator<? extends ITPS> itr = srs.rangeQuery(schema,
                null/* fromKey */, null/* toKey */, 1/* capacity */,
                SparseRowStore.MAX_TIMESTAMP, null/*filter*/);
        
        {
            
            assertTrue(itr.hasNext());

            final ITPS tps = itr.next();
         
            assertTrue( tps != null );
            
            assertEquals(1L, tps.get("Id").getValue());
            assertEquals("Bryan", tps.get("Name").getValue());
            assertEquals("NC", tps.get("State").getValue());
            
        }
        
        {

            /*
             * FIXME The test will fail here since continuation queries are not
             * implemented.
             */
            assertTrue(itr.hasNext());

            final ITPS tps = itr.next();
         
            assertTrue( tps != null );
            
            assertEquals(2L, tps.get("Id").getValue());
            assertEquals("Mike", tps.get("Name").getValue());
            assertEquals("UT", tps.get("State").getValue());
            
        }

        assertFalse(itr.hasNext());
        
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

}
