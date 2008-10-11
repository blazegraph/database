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
 * Created on Jan 22, 2008
 */

package com.bigdata.sparse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.Iterator;

import junit.framework.TestCase2;

import com.bigdata.sparse.TPS.TPV;

/**
 * Test of {@link TPS} (property timestamp set implementation).
 * 
 * @todo write tests for {@link TPS#iterator()}
 * 
 * @todo write tests for {@link TPS#asMap(long, com.bigdata.sparse.TPS.INameFilter)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTPS extends TestCase2 implements IRowStoreConstants {

    /**
     * 
     */
    public TestTPS() {
    }

    /**
     * @param arg0
     */
    public TestTPS(String arg0) {
        super(arg0);
    }

    final Schema schema = new Schema("testSchema", "pkey", KeyType.Unicode);
    
    final TPS tps = new TPS(schema,0L/*timestamp*/);
    
    final long t0 = 0L;
    final long t1 = 10L;
    final long t2 = 20L;
    final long t3 = 30L;
    final long tmax = Long.MAX_VALUE;
    
    /**
     * Test get/set semantics. This tests retrieval of the most current value
     * for a property when (a) none was ever bound; (b) it was bound once; (c)
     * when it was bound and then rebound using the same timestamp; (d) it was
     * bound and then rebound using an earlier timestamp; (e) when it was bound
     * and then rebound using a later timestamp; and (d) it was bound and then
     * deleted (rebound as null at a later timestamp).
     */
    public void test_getSet_neverBound() {
        
        assertEquals("size", 0, tps.size());

        // read current value before any value is set.
        assertNull(tps.get("foo").getValue());

        // read value as of a timestamp when none has been set.
        assertEquals(null,tps.get("foo", t1).getValue());
        
    }
    
    public void test_getPrimaryKey() {
        
        // note: null since not bound.
        assertEquals(null,tps.getPrimaryKey());

        // set the primary key.
        tps.set(schema.getPrimaryKeyName(), t1, "ABC");

        // note: it is now not bound.
        assertEquals("ABC",tps.getPrimaryKey());

    }

    public void test_getSet_boundOnce() {
        
        assertEquals("size", 0, tps.size());

        // read current value before any value is set.
        assertNull(tps.get("foo").getValue());

        // read value as of a timestamp when none has been set.
        assertEquals(null,tps.get("foo", t1).getValue());

        // set value as of that timestamp.
        tps.set("foo", t1, "bar");

        // read value as of that timestamp.
        assertEquals("bar",tps.get("foo", t1).getValue());
        
        // read current value.
        assertEquals("bar", tps.get("foo").getValue());
        
    }
    
    public void test_getSet_boundReboundSameTimestamp() {
        
        assertEquals("size", 0, tps.size());

        // read current value before any value is set.
        assertNull(tps.get("foo").getValue());

        // read value as of a timestamp when none has been set.
        assertEquals(null,tps.get("foo", t1).getValue());

        // set value as of that timestamp.
        tps.set("foo", t1, "bar");

        // read value as of that timestamp.
        assertEquals("bar",tps.get("foo", t1).getValue());
        
        // read current value.
        assertEquals("bar", tps.get("foo").getValue());

        /*
         * rebind the value using the same timestamp.
         */
        
        // set value as of that timestamp.
        tps.set("foo", t1, "baz");

        // read value as of that timestamp.
        assertEquals("baz",tps.get("foo", t1).getValue());
        
        // read current value.
        assertEquals("baz", tps.get("foo").getValue());

    }
    
    public void test_getSet_boundReboundEarlierTimestamp() {
        
        assertEquals("size", 0, tps.size());

        // read current value before any value is set.
        assertNull(tps.get("foo").getValue());

        // read value as of a timestamp when none has been set.
        assertEquals(null,tps.get("foo", t2).getValue());

        // set value as of that timestamp.
        tps.set("foo", t2, "bar");

        // read value as of that timestamp.
        assertEquals("bar",tps.get("foo", t2).getValue());
        
        // read current value.
        assertEquals("bar", tps.get("foo").getValue());

        /*
         * rebind the value using an earlier timestamp.
         */
        
        // set value as of that timestamp.
        tps.set("foo", t1, "baz");

        // read value as of that timestamp.
        assertEquals("baz",tps.get("foo", t1).getValue());
        
        // read current value.
        assertEquals("bar", tps.get("foo").getValue());

        // read the value @ t2
        assertEquals("bar",tps.get("foo", t2).getValue());

    }
    
    public void test_getSet_boundReboundLaterTimestamp() {
        
        assertEquals("size", 0, tps.size());

        // read current value before any value is set.
        assertNull(tps.get("foo").getValue());

        // read value as of a timestamp when none has been set.
        assertEquals(null,tps.get("foo", t1).getValue());

        // set value as of that timestamp.
        tps.set("foo", t1, "bar");

        // read value as of that timestamp.
        assertEquals("bar",tps.get("foo", t1).getValue());
        
        // read current value.
        assertEquals("bar", tps.get("foo").getValue());

        /*
         * rebind the value using a later timestamp.
         */
        
        // set value as of that timestamp.
        tps.set("foo", t2, "baz");

        // read value as of that timestamp.
        assertEquals("baz",tps.get("foo", t2).getValue());
        
        // read current value.
        assertEquals("baz", tps.get("foo").getValue());

        // read the previous value.
        assertEquals("bar",tps.get("foo", t1).getValue());

    }
    
    public void test_getSet_boundDeleted() {
        
        assertEquals("size", 0, tps.size());

        // read current value before any value is set.
        assertNull(tps.get("foo").getValue());

        // read value as of a timestamp when none has been set.
        assertEquals(null,tps.get("foo", t1).getValue());

        // set value as of that timestamp.
        tps.set("foo", t1, "bar");

        // read value as of that timestamp.
        assertEquals("bar",tps.get("foo", t1).getValue());
        
        // read current value.
        assertEquals("bar", tps.get("foo").getValue());

        /*
         * delete (rebind the value as null using a later timestamp).
         */
        
        // set value as of that timestamp.
        tps.set("foo", t2, null);

        // read value as of that timestamp.
        assertEquals(null,tps.get("foo", t2).getValue());
        
        // read current value.
        assertEquals(null, tps.get("foo").getValue());

        // read the previous value.
        assertEquals("bar",tps.get("foo", t1).getValue());
        
    }

    /**
     * Tests a series of bindings for one property.
     * 
     * @throws ClassNotFoundException 
     * @throws IOException 
     */
    public void test_roundTrip01() throws IOException, ClassNotFoundException {

        assertRoundTrip(tps);
        
        tps.set("foo", t1, "bar");

        assertRoundTrip(tps);

        tps.set("foo", t2, "baz");

        assertRoundTrip(tps);

        tps.set("foo", t3, null);

        assertRoundTrip(tps);

    }

    /**
     * Tests a series of bindings for one property where each binding of the
     * property uses a different {@link ValueType}.
     */
    public void test_roundTrip02() throws IOException, ClassNotFoundException {

        // empty
        assertRoundTrip(tps);
        
        // Integer
        tps.set("foo", 1L, Integer.valueOf(12));
        assertRoundTrip(tps);

        // Long
        tps.set("foo", 2L, Float.valueOf(12L));
        assertRoundTrip(tps);
        
        // Float
        tps.set("foo", 3L, Float.valueOf(12.0f));
        assertRoundTrip(tps);
        
        // Double
        tps.set("foo", 4L, Double.valueOf(12.0d));
        assertRoundTrip(tps);

        // Unicode
        tps.set("foo", 5L, "bar");
        assertRoundTrip(tps);

        // Date
        tps.set("foo", 6L, new Date(System.currentTimeMillis()));
        assertRoundTrip(tps);

        // byte[].
        tps.set("foo", 7L, new byte[]{1,2,3});
        assertRoundTrip(tps);

    }

    /**
     * Unit tests for {@link TPS#currentRow(INameFilter)} 
     */
    public void test_currentRow() {

        // verify when all property values pass the filter.
        {

            final TPS expected = new TPS(schema, 0L/* writeTime */);

            expected.set("foo", 1L, "bar");
            expected.set("goo", 2L, "baz");

            assertEquals(expected, expected.currentRow());
            
        }
 
        // verify when a property value gets filtered out by a later binding.
        {

            final TPS tmp = new TPS(schema, 0L/* writeTime */);

            tmp.set("foo", 1L, "bar"); // will be filtered out.
            tmp.set("foo", 2L, "bat");
            tmp.set("goo", 2L, "baz");

            final TPS expected = new TPS(schema, 0L/* writeTime */);

            expected.set("foo", 2L, "bat");
            expected.set("goo", 2L, "baz");

            assertEquals(expected, tmp.currentRow());

        }
        
        /*
         * verify when a property is filtered out by name.
         */
        {

            final TPS tmp = new TPS(schema, 0L/* writeTime */);

            tmp.set("foo", 1L, "bar");
            tmp.set("foo", 2L, "bat"); 
            tmp.set("goo", 2L, "baz"); // will be filtered out.

            final TPS expected = new TPS(schema, 0L/* writeTime */);

            expected.set("foo", 2L, "bat"); 

            assertEquals(expected, tmp.currentRow(new SingleColumnFilter("foo")));

        }
        
    }
    
    /**
     * Unit tests for {@link TPS#filter(long, long, INameFilter)}
     */
    public void test_filter() {

        // verify when an earlier property value gets filtered out by [fromTime].
        {

            final TPS tmp = new TPS(schema, 0L/* writeTime */);

            tmp.set("foo", 1L, "bar"); // will be filtered out.
            tmp.set("foo", 2L, "bat");
            tmp.set("goo", 2L, "baz");

            final TPS expected = new TPS(schema, 0L/* writeTime */);

            expected.set("foo", 2L, "bat");
            expected.set("goo", 2L, "baz");

            assertEquals(expected, tmp
                    .filter(2L/* fromTime */, MAX_TIMESTAMP/*toTime*/));

        }

        /*
         * verify when a later property value is filter out because it is GTE
         * the [toTime].
         */
        {

            final TPS tmp = new TPS(schema, 0L/* writeTime */);

            tmp.set("foo", 1L, "bar");
            tmp.set("foo", 2L, "bat"); // will be filtered out.
            tmp.set("goo", 2L, "baz"); // will be filtered out.

            final TPS expected = new TPS(schema, 0L/* writeTime */);

            expected.set("foo", 1L, "bar");

            assertEquals(expected, tmp
                    .filter(MIN_TIMESTAMP/* fromTime */, 2L/* toTime */));

        }

        /*
         * verify when a property is filtered out by name.
         */
        {

            final TPS tmp = new TPS(schema, 0L/* writeTime */);

            tmp.set("foo", 1L, "bar");
            tmp.set("foo", 2L, "bat"); 
            tmp.set("goo", 2L, "baz"); // will be filtered out.

            final TPS expected = new TPS(schema, 0L/* writeTime */);

            expected.set("foo", 1L, "bar");
            expected.set("foo", 2L, "bat"); 

            assertEquals(expected, tmp.filter(new SingleColumnFilter("foo")));

        }

    }
    
    /**
     * Test helper for (de-)serialization tests.
     * 
     * @param expected
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void assertRoundTrip(TPS expected) throws IOException, ClassNotFoundException {

        final byte[] serialized;
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            ObjectOutputStream oos = new ObjectOutputStream(baos);

            expected.writeExternal(oos);

            oos.flush();

            serialized = baos.toByteArray();
        }
        
        final TPS actual;
        {
            
            actual = new TPS(); // deserialization ctor.
            
            ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
            
            ObjectInputStream ois = new ObjectInputStream(bais);
            
            actual.readExternal(ois);
            
        }
        
        assertEquals(expected,actual);
        
    }
    
    /**
     * 
     */
    protected void assertEquals(TPV expected, TPV actual) {

        assertEquals("schema", expected.getSchema(), actual.getSchema());

        assertEquals("property", expected.getName(), actual.getName());

        assertEquals("timestamp", expected.getTimestamp(), actual.getTimestamp());
        
        if(expected.getValue() instanceof byte[]) {

            // Note: case necessary to invoke appropriate comparison method.

            assertEquals("value", (byte[]) expected.getValue(), (byte[]) actual
                    .getValue());

        } else {

            assertEquals("value", expected.getValue(), actual.getValue());
            
        }

    }
    
    protected void assertEquals(String msg,Schema expected, Schema actual) {
        
        assertEquals(msg+".name", expected.getName(),actual.getName());
        
        assertEquals(msg+".primaryKey", expected.getPrimaryKeyName(),actual.getPrimaryKeyName());
        
        assertEquals(msg+".primaryKeyType", expected.getPrimaryKeyType(),actual.getPrimaryKeyType());
        
    }
    
    /**
     * Asserts the same schema and the same tuples.
     * 
     * @param expected
     * @param actual
     */
    protected void assertEquals(TPS expected, TPS actual) {
        
        assertEquals("schema", expected.getSchema(), actual.getSchema());
        
        assertEquals("size", expected.size(), actual.size());

        Iterator<ITPV> eitr = expected.iterator(); 

        Iterator<ITPV> aitr = actual.iterator(); 
        
        while(eitr.hasNext()) {

            assertTrue(aitr.hasNext());
            
            assertEquals((TPV)eitr.next(), (TPV)aitr.next());
            
        }
        
        assertFalse(aitr.hasNext());
        
    }
    
}
