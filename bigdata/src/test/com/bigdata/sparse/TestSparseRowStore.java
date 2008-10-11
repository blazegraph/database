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
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.ProxyTestCase;
import com.bigdata.sparse.TPS.TPV;
import com.bigdata.util.CSVReader;

/**
 * Test suite for {@link SparseRowStore}.
 * <p>
 * Note: A lot of the pragmatic tests are being done by the
 * {@link BigdataFileSystem} which uses the {@link SparseRowStore} for its file
 * metadata index.
 * 
 * @todo test write, delete, and row scan with bad arguments.
 * 
 * @todo verify that the atomic row scan correctly handles fromTime and toTime
 *       since it uses its own logic for reading.
 * 
 * @todo verify that the "current" row for read(), get() and scan() recognizes a
 *       deleted property value and does not accept the successor of that
 *       deleted value in its place.
 * 
 * @todo test with auto-generated timestamps.
 * 
 * @todo test with application generated timestamps.
 * 
 * @todo test read of historical revision (1st, nth, last).
 * 
 * @todo test history policy (expunging of entries). this has not been handled
 *       yet, but it will probably amount to row scan where the fromTime is
 *       specified.
 *       
 * @todo It is not as easy to formulate an expunge based on the #of {@link TPV}s
 *       to retain.
 * 
 * @todo specialized compression for the keys using knowledge of schema and
 *       column names? can we directly produce a compressed representation that
 *       is order preserving?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSparseRowStore extends ProxyTestCase<IIndexManager> implements
        IRowStoreConstants {

    protected IIndex getIndex(IIndexManager store) {

        // name by the unit test.
        final String name = getName();
        
        final long timestamp = ITx.UNISOLATED;
        
        IIndex ndx = store.getIndex(name, timestamp);
        
        if (ndx == null) {

            if (INFO)
                log.info("Registering index: " + name);
            
            store.registerIndex(new IndexMetadata(name, UUID.randomUUID()));

            ndx = store.getIndex(name, timestamp);
            
        }
        
        return ndx;
        
    }
    
    /**
     * 
     */
    public TestSparseRowStore() {
    
        super();
        
    }

    /**
     * @param name
     */
    public TestSparseRowStore(String name) {
        
        super(name);
        
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

        final IIndexManager store = getStore();
        
        try {
        
        final String resourceName = "com/bigdata/sparse/test.csv";
        
        final String charSet = "ASCII";
        
        final Schema schema = new Schema("Employee","Id",KeyType.Long);
        
        /*
         * The name of the column that is used as the column value timestamp.
         */
        final String timestampColumn = "DateOfHire";

        final IIndex ndx = getIndex(store);
        
        final SparseRowStore srs = new SparseRowStore(ndx);
        
        final InputStream is = getTestInputStream(resourceName);

        final CSVReader r = new CSVReader(is,charSet);
        
        /*
         * The ground truth data read from the test resource.
         */
        final Vector<Map<String,Object>> rows = new Vector<Map<String,Object>>();
        
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
                    final Object primaryKey = row.get(schema.getPrimaryKeyName());

                    // read the row from the store.
                    final ITPS tps = srs
                            .read(schema, primaryKey,
                                timestamp, timestamp + 1, null/* filter */);

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
            
            final ITupleIterator itr = ndx.rangeIterator();
            
            while(itr.hasNext()) {
            
                final ITuple tuple = itr.next();
                
                final byte[] val = tuple.getValue();
                
                final byte[] key = tuple.getKey();

                final KeyDecoder keyDecoder = new KeyDecoder(key);
                
                if(INFO)
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
                
                final Map<String,Object> expected = rows.get(i);
                
                // extract the primary key for this row.
                final Object primaryKey = expected.get(schema.getPrimaryKeyName());
                
                /*
                 * Use the date of hire column as the timestamp on the record.
                 */
                final long timestamp = ((Date)expected.get(timestampColumn)).getTime();

                // read the row from the store.
                    final ITPS tps = srs.read(schema, primaryKey, timestamp,
                            timestamp + 1, null/* filter */);
                
                assertNotNull("No such row: "+primaryKey,tps);
                
                assertSameValues(expected,tps.asMap());
                
            }
            
        }
        
        } finally {
            
            try {store.destroy();} catch(Throwable t) {log.error(t);}
            
        }
        
    }

    /**
     * Test that a read for a row that does not exist returns <code>null</code>.
     */
    public void test_read_noSuchRow() {

        final IIndexManager store = getStore();

        try {

            final Schema schema = new Schema("Employee", "Id", KeyType.Long);

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

            assertNull(srs.read(schema, Long.valueOf(0L)));

        } finally {

            try {
                store.destroy();
            } catch (Throwable t) {
                log.error(t);
            }

        }

    }

    /**
     * Test of correct rejection of illegal parameters for logical row read
     * operations.
     */
    public void test_read_correctRejection() {
       
        final Schema schema = new Schema("Employee", "Id", KeyType.Long);

        final Long primaryKey = 1L;
        
        final long fromTime = MIN_TIMESTAMP;
        
        final long toTime = MAX_TIMESTAMP;
        
        final INameFilter filter = null;
        
        final IIndexManager store = getStore();
        
        try {
            
            final SparseRowStore srs = new SparseRowStore(getIndex(store));

            // no schema.
            fail(new Callable() {

                public Object call() {

                    srs.read(null/* schema */, primaryKey );

                    return null;

                }

            }, IllegalArgumentException.class);
            
            // no primary key.
            fail(new Callable() {

                public Object call() {

                    srs.read(schema, null);

                    return null;

                }

            }, IllegalArgumentException.class);
            
            // no schema.
            fail(new Callable() {

                public Object call() {

                    srs.read(null/*schema*/, primaryKey, fromTime, toTime, filter);

                    return null;

                }

            }, IllegalArgumentException.class);

            // no primary key.
            fail(new Callable() {

                public Object call() {

                    srs.read(schema, null/*primaryKey*/, fromTime, toTime, filter);

                    return null;

                }

            }, IllegalArgumentException.class);
            
            // bad fromTime
            fail(new Callable() {

                public Object call() {

                    srs.read(schema, primaryKey, CURRENT_ROW, 1L, filter);

                    return null;

                }

            }, IllegalArgumentException.class);

            // bad fromTime.
            fail(new Callable() {

                public Object call() {

                    srs.read(schema, primaryKey, -1L, 1L, filter);

                    return null;

                }

            }, IllegalArgumentException.class);

            // bad toTime
            fail(new Callable() {

                public Object call() {

                    srs.read(schema, primaryKey, 2L, -1L, filter);

                    return null;

                }

            }, IllegalArgumentException.class);

            // bad toTime
            fail(new Callable() {

                public Object call() {

                    srs.read(schema, primaryKey, 2L, 1L, filter);

                    return null;

                }

            }, IllegalArgumentException.class);

        } finally {
            
            store.destroy();
            
        }
        
    }
    
    /**
     * Simple test of write and read back of logical rows.
     */
    public void test_readWrite() {

        final Schema schema = new Schema("Employee", "Id", KeyType.Long);

        IIndexManager store = getStore();

        try {

            {

                final SparseRowStore srs = new SparseRowStore(getIndex(store));

                {

                    final Map<String, Object> propertySet = new HashMap<String, Object>();

                    propertySet.put("Id", 1L);
                    propertySet.put("Name", "Bryan");
                    propertySet.put("State", "NC");

                    final Map<String, Object> actual = srs.write(schema,
                            propertySet, 1L/* timestamp */);

                    assertSameValues(propertySet, actual);

                }

                // verify row.
                {

                    final Map<String, Object> row = srs
                            .read(schema, 1L/* primaryKey */);

                    assertNotNull(row);

                    assertEquals(1L, row.get("Id"));
                    assertEquals("Bryan", row.get("Name"));
                    assertEquals("NC", row.get("State"));

                }

                {

                    final Map<String, Object> propertySet = new HashMap<String, Object>();

                    propertySet.put("Id", 2L);
                    propertySet.put("Name", "Mike");
                    propertySet.put("State", "UT");

                    final Map<String, Object> actual = srs.write(schema,
                            propertySet, 2L/* timestamp */);

                    assertSameValues(propertySet, actual);

                }

                // re-verify 1st row.
                {

                    final Map<String, Object> row = srs
                            .read(schema, 1L/* primaryKey */);

                    assertNotNull(row);

                    assertEquals(1L, row.get("Id"));
                    assertEquals("Bryan", row.get("Name"));
                    assertEquals("NC", row.get("State"));

                }

                // verify 2nd row.
                {

                    final Map<String, Object> row = srs
                            .read(schema, 2L/* primaryKey */);

                    assertNotNull(row);

                    assertEquals(2L, row.get("Id"));
                    assertEquals("Mike", row.get("Name"));
                    assertEquals("UT", row.get("State"));

                }

            }

            // re-open.
            store = reopenStore(store);

            // re-verify the rows.
            {

                final SparseRowStore srs = new SparseRowStore(getIndex(store));

                // verify one row.
                {

                    final Map<String, Object> row = srs
                            .read(schema, 1L/* primaryKey */);

                    assertTrue(row != null);

                    assertEquals(1L, row.get("Id"));
                    assertEquals("Bryan", row.get("Name"));
                    assertEquals("NC", row.get("State"));

                }

                // verify the other row.
                {

                    final Map<String, Object> row = srs
                            .read(schema, 2L/* primaryKey */);

                    assertTrue(row != null);

                    assertEquals(2L, row.get("Id"));
                    assertEquals("Mike", row.get("Name"));
                    assertEquals("UT", row.get("State"));

                }

            }

        } finally {

            try {
                store.destroy();
            } catch (Throwable t) {
                log.error(t);
            }

        }

    }
    
    /**
     * Test the semantics of timestamps associated with property values by
     * atomic row writes.
     * <p>
     * Each row write is assigned a timestamp and the property values written in
     * that atomic operation all get the assigned timestamp. The same value may
     * be overwritten and the new version is always the one with the most
     * current timestamp.
     * <p>
     * This verifies ... FIXME javadoc
     * 
     * 
     * @todo test with client assigned timestamp, server assigned timestamp, and
     *       globally assigned timestamp.
     */
    public void test_timestampSemantics() {

        final Schema schema = new Schema("Employee", "Id", KeyType.Long);

        final IIndexManager store = getStore();
        
        try {

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

            final long timestamp1;
            {
                final Map<String, Object> propertySet = new HashMap<String, Object>();

                propertySet.put("Id", 1L); // primary key.
                propertySet.put("Name", "Bryan");
                propertySet.put("State", "DC");

                final long writeTime = 1L;
                
                final TPS actual = srs
                        .write(schema, propertySet, writeTime,
                                null/* filter */, null/* precondition */);

                // assigned write timestamp.
                timestamp1 = actual.getWriteTimestamp();

                // verify that the given time was assigned.
                assertEquals(timestamp1, writeTime);

                // post-condition state.
                assertTrue(Long.valueOf(1L).equals(actual.get("Id").getValue()));
                assertTrue("Bryan".equals(actual.get("Name").getValue()));
                assertTrue("DC".equals(actual.get("State").getValue()));
                
            }

            // overwrite a property value.
            final long timestamp2;
            {
                final Map<String, Object> propertySet = new HashMap<String, Object>();

                propertySet.put("Id", 1L); // primary key.
                propertySet.put("State", "NC"); // update this value.

                final long writeTime = 2L;
                
                final TPS actual = srs
                        .write(schema, propertySet, writeTime,
                                null/* filter */, null/* precondition */);

                // the assigned write time.
                timestamp2 = actual.getWriteTimestamp();
                
                // verify that the given time was assigned.
                assertEquals(timestamp2, writeTime);

                // post-condition state.
                assertTrue(Long.valueOf(1L).equals(actual.get("Id").getValue()));
                assertTrue("Bryan".equals(actual.get("Name").getValue()));
                assertTrue("NC".equals(actual.get("State").getValue()));
                
            }

            // verify read back of the current row state.
            {

                final ITPS actual = srs.read(schema, 1L/* primaryKey */,
                        MIN_TIMESTAMP, CURRENT_ROW,
                        null/* filter */);
                
                final Map<String, Object> row = actual.asMap();

                assertTrue(row != null);

                // @todo using assertSameValues is more secure as it check the #of values also.
                assertEquals(1L, row.get("Id"));
                assertEquals("Bryan", row.get("Name"));
                assertEquals("NC", row.get("State"));
                
            }

            // verify read back of row as of [timestamp1].
            {

                final ITPS actual = srs.read(schema, 1L/* primaryKey */,
                        MIN_TIMESTAMP, timestamp1 + 1, null/* filter */);
                
                final Map<String, Object> row = actual.asMap();

                assertTrue(row != null);

                assertEquals(1L, row.get("Id"));
                assertEquals("Bryan", row.get("Name"));
                assertEquals("DC", row.get("State"));

            }

            // verify read back of row as of [timestamp2].
            {

                final Map<String, Object> row = srs
                        .read(schema, 1L/* primaryKey */, MIN_TIMESTAMP,
                                timestamp2 + 1, null/* filter */).asMap();

                assertTrue(row != null);

                assertEquals(1L, row.get("Id"));
                assertEquals("Bryan", row.get("Name"));
                assertEquals("NC", row.get("State"));

            }

            // @todo verify row scan sees only the most current state of the row.
            
            // @todo verify row scan as of [timestamp0] see the 1st row state.

            // @todo verify row scan as of [timestamp1] see the 2nd row state.
            
        } finally {
            
            try {
                store.destroy();
            } catch (Throwable t) {
                log.error(t);
            }
            
        }
        
    }
    
    /**
     * Test of {@link IPrecondition} handling for atomic writes.
     */
    public void test_writeWithPrecondition() {

        final IIndexManager store = getStore();

        try {
        
        final Schema schema = new Schema("Employee", "Id", KeyType.Long);

        final SparseRowStore srs = new SparseRowStore(getIndex(store));

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
        
        } finally {
            
            try {store.destroy();} catch(Throwable t) {log.error(t);}
            
        }

    }

    /**
     * Test of auto-increment behavior.
     * 
     * @todo rework this test once auto-inc is allowed for the primary key.
     */
    public void test_autoInc() {

        final IIndexManager store = getStore();

        try {

            final Schema schema = new Schema("Employee", "Id", KeyType.Long);

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

            {

                final Map<String, Object> propertySet = new HashMap<String, Object>();

                propertySet.put("Id", 1L);
                propertySet.put("Name", "Bryan");
                propertySet.put("State", "DC");
                propertySet.put("Counter", AutoIncLongCounter.INSTANCE);

                final Map<String,Object> actual = srs.write(schema, propertySet);

                final Map<String,Object> expected = new HashMap<String, Object>(propertySet);
                expected.put("Counter", 0L); // expected value assigned by auto-inc.
                
                assertSameValues(expected, actual);
                
            }

            {

                final Map<String, Object> propertySet = new HashMap<String, Object>();

                propertySet.put("Id", 1L);
                propertySet.put("State", "NC");
                propertySet.put("Counter", AutoIncLongCounter.INSTANCE);

                final Map<String,Object> actual = srs.write(schema, propertySet);

                final Map<String,Object> expected = new HashMap<String, Object>(propertySet);
                expected.put("Name", "Bryan"); // read back from the store.
                expected.put("Counter", 1L); // expected value assigned by auto-inc.
                
                assertSameValues(expected, actual);

            }

            /*
             * Verify read back with iterator.
             */
            final Iterator<? extends ITPS> itr = srs.rangeIterator(schema);

            {

                assertTrue(itr.hasNext());

                final ITPS actual = itr.next();

                assertTrue(actual != null);

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", 1L);
                expected.put("Name", "Bryan");
                expected.put("State", "NC");
                expected.put("Counter", 1L);

                assertSameValues(expected, actual.asMap());

            }

            assertFalse(itr.hasNext());

            /*
             * Delete the logical row, verify that it is "gone", and and verify
             * that another auto-inc write will (a) re-create the logical row;
             * and (b) restart the counter at zero.
             */
            {
                
                // delete the row.
                {
                    final ITPS actual = srs.delete(schema, 1L);

                    final Map<String, Object> expected = new HashMap<String, Object>();

                    expected.put("Id", 1L);
                    expected.put("Name", "Bryan");
                    expected.put("State", "NC");
                    expected.put("Counter", 1L);

                    // verify pre-delete values.
                    assertSameValues(expected, actual.asMap());
                    
                }
                
                // verify gone.
                {
                    
                    assertSameValues(new HashMap<String, Object>(), srs.read(
                            schema, 1L));
                    
                }
                
                // verify auto-inc with same primaryKey.
                {

                    final Map<String, Object> propertySet = new HashMap<String, Object>();

                    propertySet.put("Id", 1L);
                    propertySet.put("Counter", AutoIncLongCounter.INSTANCE);
                    
                    final Map<String,Object> actual = srs.write(schema, propertySet,
                            AUTO_TIMESTAMP_UNIQUE);

                    final Map<String, Object> expected = new HashMap<String, Object>();

                    expected.put("Id", 1L);
                    expected.put("Counter", 0L);

                    // verify pre-delete values.
                    assertSameValues(expected, actual);

                    /*
                     * @todo verify the entire history for the logical row (the
                     * TPS, not just the map view).
                     */
                    
                }

            }

            /*
             * Delete the counter and verify that another auto-inc write will
             * restart the counter at zero. 
             */
            {
                
                // delete the counter (not the row).
                {
                    final Map<String, Object> propertySet = new HashMap<String, Object>();
                    
                    propertySet.put("Id", 1L);
                    propertySet.put("Name", "Bryan");// write this field.
                    propertySet.put("State", "NC");// write this field.
                    propertySet.put("Counter", null);// delete the counter.
                    
                    final Map<String,Object> actual = srs.write(schema, propertySet,
                            AUTO_TIMESTAMP_UNIQUE);

                    final Map<String, Object> expected = new HashMap<String, Object>();

                    expected.put("Id", 1L);
                    expected.put("Name", "Bryan");
                    expected.put("State", "NC");
                    // Note: [Counter] is gone (deleted).

                    // verify post-delete of counter state
                    assertSameValues(expected, actual);

                }

                // verify read-back
                {
                    final Map<String,Object> actual = srs.read(schema,1L);
                    
                    final Map<String, Object> expected = new HashMap<String, Object>();

                    expected.put("Id", 1L);
                    expected.put("Name", "Bryan");
                    expected.put("State", "NC");

                    // verify read-back
                    assertSameValues(expected, actual);
                    
                }

                // verify increment restarts the counter at zero.
                {
                 
                    final Map<String, Object> propertySet = new HashMap<String, Object>();

                    propertySet.put("Id", 1L);
                    propertySet.put("Counter", AutoIncLongCounter.INSTANCE);
                    
                    final Map<String,Object> actual = srs.write(schema, propertySet,
                            AUTO_TIMESTAMP_UNIQUE);
                    
                    final Map<String, Object> expected = new HashMap<String, Object>();

                    expected.put("Id", 1L);
                    expected.put("Name", "Bryan");
                    expected.put("State", "NC");
                    expected.put("Counter", 0L);

                    // verify post-inc values.
                    assertSameValues(expected, actual);
                }
            }
            
        } finally {

            try {store.destroy();} catch(Throwable t) {log.error(t);}

        }
        
    }
    
    /**
     * Test of a logical row scan.
     * 
     * @todo Write a variant in which we test with multiple index partitions to
     *       verify that the logical row scan correctly crosses the various
     *       index partitions. Do this for reverse traversal of logical rows
     *       also.
     */
    public void test_rowScan() {

        final IIndexManager store = getStore();

        try {

            final Schema schema = new Schema("Employee", "Id", KeyType.Long);

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

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

            final Iterator<? extends ITPS> itr = srs.rangeIterator(schema,
                    null/* fromKey */, null/* toKey */);

            {

                assertTrue(itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                assertEquals(1L, tps.get("Id").getValue());
                assertEquals("Bryan", tps.get("Name").getValue());
                assertEquals("NC", tps.get("State").getValue());

            }

            {

                assertTrue(itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                assertEquals(2L, tps.get("Id").getValue());
                assertEquals("Mike", tps.get("Name").getValue());
                assertEquals("UT", tps.get("State").getValue());

            }

            assertFalse(itr.hasNext());

        } finally {

            try {store.destroy();} catch(Throwable t) {log.error(t);}

        }
        
    }
    
    /**
     * Test of a logical row scan using a key range limit.
     * 
     * FIXME test with fromTime/toTime limits, including only the current row,
     * when there are other data that should not be read.
     */
    public void test_rowScan_withKeyRange() {

        final IIndexManager store = getStore();

        try {

            final Schema schema = new Schema("Employee", "Id", KeyType.Long);

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

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

                final Iterator<? extends ITPS> itr = srs.rangeIterator(schema, 1L, 2L);

                assertTrue(itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                assertEquals(1L, tps.get("Id").getValue());
                assertEquals("Bryan", tps.get("Name").getValue());
                assertEquals("NC", tps.get("State").getValue());

                assertFalse(itr.hasNext());

            }

            {

                Iterator<? extends ITPS> itr = srs
                        .rangeIterator(schema, 2L, null/* toKey */);

                assertTrue(itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                assertEquals(2L, tps.get("Id").getValue());
                assertEquals("Mike", tps.get("Name").getValue());
                assertEquals("UT", tps.get("State").getValue());

                assertFalse(itr.hasNext());

            }

        } finally {

            try {store.destroy();} catch(Throwable t) {log.error(t);}

        }

    }
    
    /**
     * Test of a logical row scan requiring continuation queries by forcing the
     * capacity to 1 when there are in fact two logical rows. This variant uses
     * a primary key with a fixed length data type (long).
     */
    public void test_rowScan_continuationQuery_fixedLengthPrimaryKey() {

        final IIndexManager store = getStore();

        try {

            final Schema schema = new Schema("Employee", "Id", KeyType.Long);

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

            /*
             * make sure that there is nothing in the row store (can happen if
             * the data was not deleted by the test harness).
             */
            {

                final Iterator<? extends ITPS> itr = srs.rangeIterator(schema);

                assertFalse(itr.hasNext());

            }

            // add one row.
            {

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", 1L);
                expected.put("Name", "Bryan");
                expected.put("State", "NC");

                final Map<String, Object> actual = srs.write(schema, expected);

                assertSameValues(expected, actual);

            }

            // add another row.
            {

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", 2L);
                expected.put("Name", "Mike");
                expected.put("State", "UT");

                final Map<String, Object> actual = srs.write(schema, expected);

                assertSameValues(expected, actual);

            }

            final Iterator<? extends ITPS> itr = srs.rangeIterator(schema,
                    null/* fromKey */, null/* toKey */, 1/* capacity */,
                    MIN_TIMESTAMP, CURRENT_ROW, null/* filter */);

            {

                assertTrue(itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", 1L);
                expected.put("Name", "Bryan");
                expected.put("State", "NC");
                
                assertSameValues(expected, tps.asMap());

            }

            {

                /*
                 * The test will fail here if continuation queries are not
                 * implemented.
                 */
                assertTrue("Did not issue continuation query?", itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", 2L);
                expected.put("Name", "Mike");
                expected.put("State", "UT");
                
                assertSameValues(expected, tps.asMap());

            }

            if (itr.hasNext()) {

                fail("Not expecting: " + itr.next());

            }

        } finally {

            try {
                store.destroy();
            } catch (Throwable t) {
                log.error(t);
            }

        }

    }

    /**
     * Test of a logical row scan requiring continuation queries by forcing the
     * capacity to 1 when there are in fact two logical rows. This variant uses
     * a primary key with a variable length data type (long).
     */
    public void test_rowScan_continuationQuery_variableLengthPrimaryKey() {

        final IIndexManager store = getStore();

        try {

            final Schema schema = new Schema("Employee", "Id", KeyType.Unicode);

            final SparseRowStore srs = new SparseRowStore(getIndex(store));

            /*
             * make sure that there is nothing in the row store (can happen if
             * the data was not deleted by the test harness).
             */
            {

                final Iterator<? extends ITPS> itr = srs.rangeIterator(schema);

                assertFalse(itr.hasNext());

            }

            // add one row.
            {

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", "10");
                expected.put("Name", "Bryan");
                expected.put("State", "NC");

                final Map<String, Object> actual = srs.write(schema, expected);

                assertSameValues(expected, actual);

            }

            // add another row.
            {

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", "20");
                expected.put("Name", "Mike");
                expected.put("State", "UT");

                final Map<String, Object> actual = srs.write(schema, expected);

                assertSameValues(expected, actual);

            }

            final Iterator<? extends ITPS> itr = srs.rangeIterator(schema,
                    null/* fromKey */, null/* toKey */, 1/* capacity */,
                    MIN_TIMESTAMP, CURRENT_ROW, null/* filter */);

            {

                assertTrue(itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", "10");
                expected.put("Name", "Bryan");
                expected.put("State", "NC");
                
                assertSameValues(expected, tps.asMap());
                
            }

            {

                /*
                 * The test will fail here if continuation queries are not
                 * implemented.
                 */
                assertTrue("Did not issue continuation query?", itr.hasNext());

                final ITPS tps = itr.next();

                assertTrue(tps != null);

                final Map<String, Object> expected = new HashMap<String, Object>();

                expected.put("Id", "20");
                expected.put("Name", "Mike");
                expected.put("State", "UT");
                
                assertSameValues(expected, tps.asMap());

            }

            if (itr.hasNext()) {

                fail("Not expecting: " + itr.next());

            }

        } finally {

            try {
                store.destroy();
            } catch (Throwable t) {
                log.error(t);
            }

        }

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
