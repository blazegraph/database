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
 * Created on Apr 30, 2007
 */

package com.bigdata.btree;

import java.util.Locale;

import junit.framework.TestCase2;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

/**
 * Tests for Unicode support in {@link UnicodeKeyBuilder}.
 * 
 * @todo write performance test for encoding strings, possibly in the context of
 *       parsed rdf data, and see if there are any easy wins in how the encoding
 *       to a sort key is handled or in alignment of the apis.
 * 
 * @todo use ICU4J as provider for compression for unicode fields, perhaps
 *       factoring a minimal module into extSer for that purpose.
 * 
 * @todo document use of ICU for forming keys but note that this feature really
 *       belongs in the client not the server. when dividing bigdata into client -
 *       server APIs, ICU will go into the client API. Also, a minimum ICU
 *       module could be built to support sort key generation. Integrate support
 *       for ICU versioning into the client and perhaps into the index metadata
 *       so clients can discover which version to use when generating keys for
 *       an index, and also the locale and other parameters for the collation
 *       (PRIMARY, SECONDARY, etc). Support the JNI integration of ICU (native
 *       code for faster generation of sort keys).
 * 
 * @todo write tests in that construct keys for a triple store, quad store,
 *       column store, and the metadata index for a paritioned index.
 * 
 * @todo refactor the bulk insert tests into another test suite since they
 *       introduce a dependency on the btree implementation rather than just a
 *       focus on the correctness and performance of the key encoder.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnicodeKeyBuilder extends TestCase2 {

    /**
     * 
     */
    public TestUnicodeKeyBuilder() {
    }

    /**
     * @param arg0
     */
    public TestUnicodeKeyBuilder(String arg0) {
        super(arg0);
    }

    /**
     * ctor tests, including correct rejection.
     */
    public void test_ctor() {

        {
            UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder();
            
            assertNotNull(keyBuilder.buf);
            assertEquals(UnicodeKeyBuilder.DEFAULT_INITIAL_CAPACITY,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);

        }
        
        {
            UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder(0);
            assertNotNull(keyBuilder.buf);
            assertEquals(0,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);
        }
        
        {
            UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder(20);
            assertNotNull(keyBuilder.buf);
            assertEquals(20,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);
        }
        
        {
            UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder((RuleBasedCollator)Collator.getInstance(Locale.UK),30);
            assertNotNull(keyBuilder.buf);
            assertEquals(30,keyBuilder.buf.length);
            assertEquals(0,keyBuilder.len);
            assertNotNull(keyBuilder.collator);
        }
        
        {
            final byte[] expected = new byte[]{1,2,3,4,5,6,7,8,9,10};
            UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder((RuleBasedCollator)Collator.getInstance(Locale.UK),4,expected);
            assertNotNull(keyBuilder.buf);
            assertEquals(4,keyBuilder.len);
            assertEquals(10,keyBuilder.buf.length);
            assertTrue(expected==keyBuilder.buf);
            assertNotNull(keyBuilder.collator);
        }

        /*
         * correct rejection tests.
         */
        {
            try {
                new UnicodeKeyBuilder(-1);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
        }

        {
            try {
                new UnicodeKeyBuilder(null,20);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
        }

        {
            try {
                new UnicodeKeyBuilder((RuleBasedCollator)Collator.getInstance(Locale.UK),-20);
                fail("Expecting: "+IllegalArgumentException.class);
            } catch(IllegalArgumentException ex) {
                System.err.println("Ignoring expected exception: "+ex);
            }
        }
        
    }
    
    public void test_keyBuilder_unicode_char_key() {
        
        fail("write test");
    }
    
    public void test_keyBuilder_unicode_chars_key() {
        
        fail("write test");
    }

    /**
     * Test ability to encode unicode data into a variable length byte[] that
     * allows direct byte-by-byte comparisons which maintain the local-specific
     * sort order of the original strings.
     */
    public void test_keyBuilder_unicode_string_key_us_primary() {

        /*
         * Get the Collator for US English and set its strength to PRIMARY.
         */
        RuleBasedCollator usCollator = (RuleBasedCollator) Collator
        .getInstance(Locale.US);
            
        usCollator.setStrength(Collator.PRIMARY);
        
        assertEquals(0,usCollator.compare("abc", "ABC"));

        IKeyBuilder keyBuilder = new UnicodeKeyBuilder(usCollator,1000);
            
        byte[] key1 = keyBuilder.reset().append("abc").getKey();
        byte[] key2 = keyBuilder.reset().append("ABC").getKey();
        byte[] key3 = keyBuilder.reset().append("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));

        // all are equal using PRIMARY strength.
        assertEquals(0,BytesUtil.compareBytes(key1, key2));
        assertEquals(0,BytesUtil.compareBytes(key2, key3));
        
    }

    public void test_keyBuilder_unicode_string_key_us_identical() {

        /*
         * Get the Collator for US English and set its strength to IDENTICAL.
         */
        RuleBasedCollator usCollator = (RuleBasedCollator) Collator
        .getInstance(Locale.US);
            
        usCollator.setStrength(Collator.IDENTICAL);
        
        assertNotSame(0,usCollator.compare("abc", "ABC"));

        IKeyBuilder keyBuilder = new UnicodeKeyBuilder(usCollator,1000);
            
        byte[] key1 = keyBuilder.reset().append("abc").getKey();
        byte[] key2 = keyBuilder.reset().append("ABC").getKey();
        byte[] key3 = keyBuilder.reset().append("Abc").getKey();

        System.err.println("abc: "+BytesUtil.toString(key1));
        System.err.println("ABC: "+BytesUtil.toString(key2));
        System.err.println("Abc: "+BytesUtil.toString(key3));

        // verify ordering for IDENTICAL comparison.
        assertTrue(BytesUtil.compareBytes(key1, key2)<0);
        assertTrue(BytesUtil.compareBytes(key2, key3)>0);
        
    }

    /**
     * Tests for keys formed from the application key, a column name, and a long
     * timestamp. A zero(0) byte is used as a delimiter between components of
     * the key.
     * 
     * @todo this is not testing much yet and should be in its own test suite.
     */
    public void test_cstore_keys() {
        
        IKeyBuilder keyBuilder = new UnicodeKeyBuilder();
        
        final byte[] colname1 = keyBuilder.reset().append("column1").getKey();
        
        final byte[] colname2 = keyBuilder.reset().append("another column").getKey();
        
        final long timestamp = System.currentTimeMillis();
        
        byte[] k1 = keyBuilder.reset().append(12L).appendNul().append(colname1)
        .appendNul().append(timestamp).getKey();

        byte[] k2 = keyBuilder.reset().append(12L).appendNul().append(colname2)
        .appendNul().append(timestamp).getKey();

        System.err.println("k1="+BytesUtil.toString(k1));
        System.err.println("k2="+BytesUtil.toString(k2));

        fail("this does not test anything yet");
    }

//    /**
//     * Encodes a collection of strings, returning sort keys for those
//     * strings. Each sort key has the property that a bit-wise (or
//     * byte-wise) comparison of sort keys produced by the same collator will
//     * have the same ordering as the strings (according to the rules of the
//     * collator). Keys produced by different collators must not be mixed.
//     * The returned keys can be sorted using {@link Arrays#sort(byte[])}.
//     * 
//     * @todo the problem with the approach is that we want to sort the keys
//     * before presenting them to the btree.  However, the application needs
//     * to know which key is associated with which input string.  If we use
//     * {@link CollatorKey} then that relationship is preserved.  If we use
//     * {@link RawCollatorKey} then the code is more efficient, but we loose
//     * the relationship and it is hard to sort the data ....
//     * 
//     * create internally a tuple {string, key, id}, where id is a one up
//     * integer for the batch.  sort those tuples by key and id will provide
//     * the permutation of the original strings that will present them in 
//     * sorted order to the btree.  the btree responses will come out in the
//     * same tuple ordering.
//     * 
//     * @param collator
//     *            The collator.
//     * @param strings
//     *            The strings (in).
//     *            
//     * @return The sort keys.
//     */
//    public static Tuple[] encodeString(RuleBasedCollator collator,String[] strings) {
//        
//        Tuple[] tuples = new Tuple[strings.length];
//
//        IKeyBuilder keyBuilder = new UnicodeKeyBuilder(collator,Bytes.kilobyte32);
//        
//        for(int i=0; i<strings.length; i++) {
//            
//            String s = strings[i];
//            
//            keyBuilder.reset();
//            
//            keyBuilder.append(s);
//            
//            // copy out the sort key from the buffer.
//            byte[] sortKey = keyBuilder.getKey();
//
//            // form tuple.
//            tuples[i] = new Tuple(i,s,sortKey);
//            
//        }
//        
//        return tuples;
//        
//    }
//
//    public static void batchInsert(BTree btree,Tuple[] tuples,boolean sorted) {
//
//        final int ntuples = tuples.length;
//
//        /*
//         * Someone must sort the tuples before presenting to the btree. In
//         * general, the client has more information, especially whether or
//         * not the data are already sorted. As an alternative, we could set
//         * a flag in the batch api or the network api by which the caller
//         * declares sorted data and sort the data when it is not already
//         * sorted.
//         */
//        if (!sorted) {
//            { // shallow copy
//                Tuple[] tuples2 = new Tuple[ntuples];
//                for (int i = 0; i < ntuples; i++) {
//                    tuples2[i] = tuples[i];
//                }
//                tuples = tuples2;
//            }
//            // sort the copy.
//            Arrays.sort(tuples, TupleComparator.INSTANCE);
//        }
//
//        byte[][] keys = new byte[ntuples][];
//        { // shallow copy of sort keys.
//            for(int i=0; i<ntuples; i++) {
//                keys[i] = tuples[i].sortKey;
//            }
//        }
//        
//        Object[] values = new Object[ntuples];
//        { // shallow copy of values paired to sort keys.
//            for(int i=0; i<ntuples; i++) {
//                values[i] = tuples[i].value;
//            }
//        }
//        
//        /*
//         * batch insert operation - no outputs.
//         */
//        btree.insert(new BatchInsert(ntuples, keys, values));
//        
//    }
//    
//    public static class Tuple implements Comparable<Tuple> {
//        public final int id;
//        public final String string;
//        public final byte[] sortKey;
//        public Object value;
//        public Tuple(int id,String string, byte[] sortKey) {
//            this.id = id;
//            this.string = string;
//            this.sortKey = sortKey;
//        }
//        public int compareTo(Tuple o) {
//            return BytesUtil.compareBytes(sortKey,o.sortKey);
//        }
//    }
//
//    public static class TupleComparator implements Comparator<Tuple> {
//        
//        public static transient final Comparator<Tuple> INSTANCE = new TupleComparator();
//        
//        public int compare(Tuple o1, Tuple o2) {
//            return BytesUtil.compareBytes(o1.sortKey,o2.sortKey);
//        }
//    }

}
