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
 * Created on Apr 30, 2007
 */

package com.bigdata.btree;

import java.util.Arrays;
import java.util.Locale;

import junit.framework.TestCase2;

import com.bigdata.rawstore.Bytes;
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
     * Test verifies that the trailing <code>nul</code> byte is not part of
     * the key when a unicode string is appended to an {@link IKeyBuilder}.
     * <p>
     * Note: The trailing <code>nul</code> byte is appended by the ICU library
     * in order to have compatibility with their C library, but it is not of
     * interest for Java processing. However, note that a <code>nul</code>
     * byte MAY be used to separate components of a complex key.
     */
    public void test_keyBuilder_unicode_String_noTrailingNul() {

        UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder();
        
        keyBuilder.append("Hello World!");
        
        byte[] key = keyBuilder.getKey();
        
        assertNotSame("Not expecting a trailing nul byte.", (byte) 0,
                key[key.length - 1]);
        
    }
    
    /**
     * Test examines the behavior when the
     * {@link SuccessorUtil#successor(String)} of an Unicode string is formed by
     * appending a <code>nul</code> character and reports an error if the
     * resulting byte[] when the key are formed compares as equal to the
     * original string from which the successor was formed.
     * <p>
     * Note: Since {@link Collator#IDENTICAL} appears to be required to
     * differentiate a trailing nul character (i.e., the successor of some
     * Unicode string), then I would strongly recommend that you form the sort
     * key first and then its successor (by appending a trailing nul).
     */
    public void test_keyBuilder_unicode_trailingNuls() {

        int[] strengths = new int[] { 
                Collator.PRIMARY,
                Collator.SECONDARY,
                Collator.TERTIARY,
                Collator.QUATERNARY,
                Collator.IDENTICAL,
                };
        
        int minStrength = -1;
        
        for(int i=0; i<strengths.length; i++) {
           
            final int strength = strengths[i];
            
            RuleBasedCollator collator = (RuleBasedCollator) Collator
                    .getInstance(Locale.getDefault());
            
            collator.setStrength(strength);
            
            if(!doSuccessorTest( "Hello World!", collator )) {
                
                log.warn("Collator does not differentiate trailing nul characters at strength="+strength);
                
            } else {
                
                minStrength = strength;
                
            }

        }
        
        assertFalse(
                "Collator will not differentiate trailing nul characters at any strength.",
                minStrength == -1); 

        System.err
                .println("Minimum strength ("+minStrength+") to differentiate trailing nul character is: "
                        + (minStrength == Collator.PRIMARY ? "PRIMARY"
                                : (minStrength == Collator.SECONDARY ? "SECONDARY"
                                        : (minStrength == Collator.TERTIARY ? "TERTIARY"
                                                : (minStrength == Collator.QUATERNARY ? "QUARERNARY"
                                                        : (minStrength == Collator.IDENTICAL ? "IDENTICAL"
                                                                : ""
                                                                        + minStrength))))));
        
    }

    /**
     * Test whether or not the {@link Collator} will differentiate Unicode
     * strings that differ only in a trailing <code>nul</code> character.
     * 
     * @param s
     *            The Unicode string.
     * @param collator
     *            The collator.
     *            
     * @return True iff the collector differenties between the string and its
     *         successor (formed by appending a nul character) in its generated
     *         sort keys.
     */
    protected boolean doSuccessorTest(String s, RuleBasedCollator collator) {

        final IKeyBuilder keyBuilder = new UnicodeKeyBuilder(collator,
                Bytes.kilobyte32);

        final String successor = SuccessorUtil.successor(s);

        // the successor is one character longer.
        assertEquals(s.length() + 1, successor.length());

        final byte[] key1 = keyBuilder.reset().append(s).getKey();

        final byte[] key2 = keyBuilder.reset().append(successor).getKey();

        // key1 MUST order less than key2.
        final int ret = BytesUtil.compareBytes(key1, key2);

        if (ret >= 0) {
            log
                    .warn("Key1 does NOT order less than successor(key1) : comparator returns "
                            + ret);
            System.err.println("text=" + s);
            System.err.println("strength=" + collator.getStrength());
            System.err.println("key1: " + Arrays.toString(key1));
            System.err.println("key2: " + Arrays.toString(key2));
            return false;
        }

        return true;
        
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
