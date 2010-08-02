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
 * Created on May 29, 2008
 */

package com.bigdata.sparse;

import java.text.Collator;
import java.util.Properties;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.ibm.icu.text.CollationKey;

/**
 * Test suite for round trip of keys as encoded by
 * {@link Schema#fromKey(com.bigdata.btree.keys.IKeyBuilder, Object)}, by
 * {@link AtomicRowWriteRead}, and as decoded by the {@link KeyDecoder}.
 * <p>
 * Note: Not all information can be fully decoded. In particular, the exact
 * schema name and the value of the primary key for Unicode {@link KeyType}s
 * can not be directly recovered from the key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyEncodeDecode extends TestCase2 {

    public TestKeyEncodeDecode() {
        
    }
    
    public TestKeyEncodeDecode(String name) {
        
        super(name);
        
    }

    /**
     * Test for primitive data types where we can round trip the primary key
     * value.
     * 
     * FIXME tests for other primitives
     * 
     * FIXME tests for Unicode
     * 
     * FIXME tests for ASCII
     * 
     * FIXME tests for Date
     * 
     * FIXME All unit tests in this class should be for all collators (e.g., an
     * abstract base class).
     */
    public void test_primitive_long() {

        final IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance(); 
        
        final Schema schema = new Schema("Employee", "Id", KeyType.Long);
        
        final Long primaryKey = 1L;
        
        final byte[] key = schema.getKey(keyBuilder, primaryKey, "Id", 12L);

        final KeyDecoder decoded = new KeyDecoder(key);
        
        assertEquals(schema.getPrimaryKeyType(), decoded.getPrimaryKeyType());

        assertEquals("Id", decoded.getColumnName());

        assertEquals(12L, decoded.getTimestamp());
        
    }

    /**
     * Unit test verifies that we can correctly locate the start of the column
     * name and decode the key when using {@link CollatorEnum#ICU}.
     */
    public void test_keyDecode_ICU() {

        final Properties props = new Properties();
        props.put(KeyBuilder.Options.COLLATOR, CollatorEnum.ICU.toString());
        final IKeyBuilder keyBuilder = new DefaultKeyBuilderFactory(props)
                .getKeyBuilder();
        assertTrue(keyBuilder.isUnicodeSupported());

        doKeyDecodeTest(keyBuilder);
//        final Schema schema = new Schema("Employee", "Id", KeyType.Unicode);
//        final String primaryKey = "1L";
//        final String column = "Id";
//        final long writeTime = 12L;
//        final byte[] key = schema.getKey(keyBuilder, primaryKey, column,
//                writeTime);
//        final KeyDecoder decoded = new KeyDecoder(key);
//
//        assertEquals(schema.getPrimaryKeyType(), decoded.getPrimaryKeyType());
//        assertEquals(column, decoded.getColumnName());
//        assertEquals(writeTime, decoded.getTimestamp());
    }

    /**
     * Unit test verifies that we can correctly locate the start of the column
     * name and decode the key when using {@link CollatorEnum#ASCII}.
     */
    public void test_keyDecode_ASCII() {

        final Properties props = new Properties();
        props.put(KeyBuilder.Options.COLLATOR,CollatorEnum.ASCII.toString());
        final IKeyBuilder keyBuilder =
            new DefaultKeyBuilderFactory(props).getKeyBuilder();
        assertFalse(keyBuilder.isUnicodeSupported());

        doKeyDecodeTest(keyBuilder);
    }

    /**
     * Unit test verifies that we can correctly locate the start of the column
     * name and decode the key when using {@link CollatorEnum#JDK}.
     * <p>
     * Note: The JDK {@link CollationKey} embeds <code>nul</code> bytes in its
     * Unicode sort keys.
     */
    public void test_keyDecode_JDK() {

        final Properties props = new Properties();
        props.put(KeyBuilder.Options.COLLATOR,CollatorEnum.JDK.toString());
        props.put(KeyBuilder.Options.USER_COUNTRY, "US");
        props.put(KeyBuilder.Options.USER_LANGUAGE, "en");
        props.put(KeyBuilder.Options.STRENGTH,Collator.TERTIARY);
        final IKeyBuilder keyBuilder =
            new DefaultKeyBuilderFactory(props).getKeyBuilder();
        assertTrue(keyBuilder.isUnicodeSupported());

        doKeyDecodeTest(keyBuilder);
        
    }
    
    /**
     * Test helper verifies that we can correctly locate the start of the column
     * name and decode the key when using a given {@link IKeyBuilder}.
     */
    protected void doKeyDecodeTest(final IKeyBuilder keyBuilder) {

        final Schema schema = new MySchema();
        final String primaryKey = "U100.lex";
        final String column = "com.bigdata.btree.keys.KeyBuilder.collator";
        final long writeTime = 1279133923566L;

        final byte[] key = schema.getKey(keyBuilder, primaryKey, column, writeTime);

        final KeyDecoder decoded = new KeyDecoder(key);

        System.err.println("decoded: "+decoded);
        
        if(SparseRowStore.schemaNameUnicodeClean) {
            
            assertEquals(schema.getName(),decoded.getSchemaName());
            
        }

        assertEquals(schema.getPrimaryKeyType(), decoded.getPrimaryKeyType());
        
        if(SparseRowStore.primaryKeyUnicodeClean) {
        
            assertEquals(primaryKey,decoded.getPrimaryKey());
            
        }

        /*
         * Note: Historically, this would fail on the column name for the JDK
         * CollatorEnum option. The problem was that the JDK CollatorEnum option
         * embeds nul bytes into the primaryKey so we are not able to correctly
         * locate the start of the column name. This was resolved with the
         * [primaryKeyUnicodeClean] option.
         */
        assertEquals(column, decoded.getColumnName());
        
        assertEquals(writeTime, decoded.getTimestamp());

    }

    /**
     * Private schema used by the unit tests.
     */
    static private class MySchema extends Schema {

        /**
         * The primary key.
         */
        public static final String NAMESPACE = MySchema.class.getPackage()
                .getName()
                + ".namespace";

        public MySchema() {

            super("my/own-schema_now.10.0", NAMESPACE, KeyType.Unicode);

        }

    }
    
}
