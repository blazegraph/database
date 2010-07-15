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

import junit.framework.TestCase2;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.relation.RelationSchema;
import java.text.Collator;
import java.util.Properties;

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
     */
    public void test_primitive_unicode() {

        IKeyBuilder keyBuilder = KeyBuilder.newUnicodeInstance();
        Schema schema = new Schema("Employee", "Id", KeyType.Unicode);
        String primaryKey = "1L";
        byte[] key = schema.getKey(keyBuilder, primaryKey, "Id", 12L);
        KeyDecoder decoded = new KeyDecoder(key);

        assertEquals(schema.getPrimaryKeyType(), decoded.getPrimaryKeyType());
        assertEquals("Id", decoded.getColumnName());
        assertEquals(12L, decoded.getTimestamp());
    }

    /**
     */
    public void test_primitive_relation_jdk() {

        Properties props = new Properties();
        props.put("com.bigdata.btree.keys.KeyBuilder.collator", CollatorEnum.JDK);
        props.put(KeyBuilder.Options.COLLATOR, "JDK");
        props.put(KeyBuilder.Options.USER_COUNTRY, "US");
        props.put(KeyBuilder.Options.USER_LANGUAGE, "en");
        props.put(KeyBuilder.Options.STRENGTH, Collator.TERTIARY);
        IKeyBuilder keyBuilder =
            new DefaultKeyBuilderFactory(props).getKeyBuilder();

        Schema schema = new RelationSchema();
        String primaryKey = "U100.lex";
        String column = "com.bigdata.btree.keys.KeyBuilder.collator";
        long writeTime = 1279133923566L;

        byte[] key = schema.getKey(keyBuilder, primaryKey, column, writeTime);
        assertEquals(key.length, 120);

        KeyDecoder decoded = new KeyDecoder(key);
        assertEquals(schema.getPrimaryKeyType(), decoded.getPrimaryKeyType());
        assertEquals("Id", decoded.getColumnName());
        assertEquals(12L, decoded.getTimestamp());

    }
}
