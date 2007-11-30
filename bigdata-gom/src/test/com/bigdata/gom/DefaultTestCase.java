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

package com.bigdata.gom;

import java.io.File;
import java.util.Properties;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;

import org.CognitiveWeb.generic.GenericAbstractTestCase;
import org.CognitiveWeb.generic.ObjectManagerFactory;
import org.CognitiveWeb.generic.core.TestAll;
import org.CognitiveWeb.generic.core.om.ObjectManager;
import org.CognitiveWeb.generic.core.om.RuntimeOptions;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Options;

/**
 * <p>
 * Test the GOM for bigdata integration.
 * </p>
 * <p>
 * This runs some bootstrap tests, the test suite defined in the generic-test
 * CVS module, and the test suite defined for the GOM implementation.
 * </p>
 */
public class DefaultTestCase extends GenericAbstractTestCase {

    public DefaultTestCase() {
        
        super();
        
    }

    public DefaultTestCase(String name) {
    
        super(name);
        
    }
    
    public static Test suite() {

        final DefaultTestCase delegate = new DefaultTestCase(); // !!!! THIS CLASS !!!!

		/*
		 * Use a proxy test suite and specify the delegate.
		 */

		ProxyTestSuite suite = new ProxyTestSuite(delegate,"bigdata GOM");

        /*
         * Bootstrap unit tests. 
         */
        suite.addTestSuite(DefaultTestCase.class);
        
		/*
		 * Pickup GOM implementation test suite.
		 */
		suite.addTest(TestAll.suite());

		/*
		 * Pickup the generic-test suite. This is a proxied test suite, so we
		 * pass in the suite which has the reference to the delegate.
		 */
		addGenericSuite(suite);

		return suite;

	}

	/**
     * <p>
     * Sets the properties for the object manager implementation and the
     * persistence store integration layer for bigdata.
     * </p>
     * 
     * @see ObjectManagerFactory#OBJECT_MANAGER_CLASSNAME
     * @see RuntimeOptions#PERSISTENT_STORE
     * @see com.bigdata.Options
     */
	public Properties getProperties(){
        
		Properties properties = super.getProperties();
        
        properties.setProperty(ObjectManagerFactory.OBJECT_MANAGER_CLASSNAME,
                ObjectManager.class.getName());
        
        properties.setProperty(RuntimeOptions.PERSISTENT_STORE,
                RuntimeOptions.PERSISTENT_STORE_BIGDATA);

        if(properties.getProperty(Options.BUFFER_MODE)==null) {

            // use the disk-based journal by default.
            
            properties.setProperty(Options.BUFFER_MODE, BufferMode.Disk.toString());
            
        }

        if (properties.getProperty(Options.FILE) == null) {
        
            // Use a temporary file by default.
//            properties.setProperty(Options.CREATE_TEMP_FILE,"true");

            properties.setProperty(Options.FILE, defaultTestDatabase);
            
            properties.setProperty(Options.DELETE_ON_EXIT,"true");

            properties.setProperty(dropBeforeTest,"true");
            
            properties.setProperty(dropAfterTest,"true");

        }
        
		return properties;
        
	}
	
	
	/**
	 * The name of the default database used by this test suite.
	 */
	protected static final String defaultTestDatabase = "GOMTest"+Options.JNL;

	/**
     * Deletes the persistent store files with the given basename.
     * The store files are generally named for the test.  Each store
     * has two files, one with a ".db" extension and one with a ".lg"
     * extension.
     */
    protected void dropStore() {

		String filename = getProperties().getProperty(
                com.bigdata.journal.Options.FILE);

		if (filename == null) {

            // Not a persistent store.
            
			return;

		}

        File file = new File(filename);
        
		if (file.exists()) {

			log.info("deleting pre-existing file: " + file);

			if( ! file.delete() ) {
				
				log.warn("Could not delete file: " + file );
				
			}

		}

	}

    /*
     * bootstrap tests.
     */
    
    /**
     * Unit tests for the correct formation of composite keys.
     */
    public void test_newCompositeKey() {

        try {

            MyBTree.newCompositeKey(null, 0L);

            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            log.info("Ignoring expected exception: " + ex);

        }

        doCompositeKeyTest(new byte[] {}, 0L);

        doCompositeKeyTest(new byte[] {}, 12L);

        doCompositeKeyTest(new byte[] { 1, 2, 3 }, 0L);

        doCompositeKeyTest(new byte[] { 1, 2, 3 }, 29L);

    }

    private void doCompositeKeyTest(byte[] in, long oid) {

        byte[] expected = KeyBuilder.newInstance().append(in).append(oid)
                .getKey();

        byte[] actual = MyBTree.newCompositeKey(in, oid);

        assertEquals(expected, actual);

    }

    /**
     * Test successor semantics for standard keys (indices that do not allow
     * duplicate keys). Keys are not coerced except for untyped property values,
     * which are coerced using generic type conversion to a String. The
     * successor of a key is always formed in the natual value space of the
     * (coerced) key, so this is the natural value space of the property value
     * except for untyped property values since those are coerced to Strings.
     * The key is then formed by converting the (coerced) key to an unsigned
     * byte[].
     * 
     * @todo test with other value types and especially with String.
     */
    public void test_successor_long() {
        
        doSuccessorTest(0L, 0L + 1);

        doSuccessorTest(1L, 1L + 1);
        
        doSuccessorTest(12L, 12L + 1);

        doSuccessorTest(-1L, -1L + 1);

        doSuccessorTest(-12L, -12L + 1);

        doSuccessorTest(Long.MIN_VALUE, Long.MIN_VALUE + 1);

        // Note: test does not make sense since long will overflow. 
        //doSuccessorTest(Long.MAX_VALUE, Long.MAX_VALUE + 1);

    }
    
    public void test_successor_char() {
        
        doSuccessorTest(new Character('m'), new Character('n'));

        /*
         * Note: '{' is the successor of 'z' ('z'+1 -> '{'). However, if you 1st
         * convert characters to Strings and then generate sort keys from those
         * Strings the successor semantics of the characters appear to be
         * violated by the successor semantics for Unicode strings!
         */
        
        doSuccessorTest(new Character('z'), new Character('{'));
           
    }
    
    /**
     * FIXME The issue is that the Unicode sort keys will be identical for any
     * String and its successor when formed by appending a trailing nul
     * character unless you are using IDENTICAL as the collator strength, in
     * which case you do not get any compression in the generated Unicode sort
     * keys (the btree will still compress leading bytes so maybe that is not so
     * bad).
     * <P>
     * Running with -Dcollator.strength=Identical is a temporary fix, but the
     * issue with how the successor is formed for strings needs to be resolved.
     * <P>
     * The probable fix is to add yet another parameter to getInternalKey(...)
     * which specifies whether the successor of the key is to be returned. The
     * key can then be converted to a sort key and a nul appended to obtain the
     * successor. Then, if necessary, the oid can be appended to break ties.
     * 
     * FIXME The other half of this problem is that you simply can not combine
     * multi-part keys such as (vartext+oid) as a single unsigned byte[] and
     * then compare the byte[]s directly - this fails to respect the end of the
     * vartext field and the oid value gets mixed up in the vartext and breaks
     * the total ordering.
     * <p>
     * One work around is to pad out the values to a maximum length in the
     * coercer and then append the oid. However this will only work for ASCII
     * and perhaps for uncompressed Unicode sort keys. Compressed Unicode sort
     * keys are always variable length, which just re-introduces the same
     * problem.
     * 
     * Note: There may be another shows that shows up as iterators that are
     * exhausted prematurely and incorrect size counts on the link set index,
     * but I can not be certain until I resolve the Unicode sort key successor
     * issue and the multi-part key issue.
     */
    public void test_successor_String() {
        
        doSuccessorTest("","\0");

        doSuccessorTest("abc","abc\0");

        doSuccessorTest("z","z\0");
        
    }

    /**
     * Verifies that a sort key derived from k1 is ordered after a sort key
     * derived from k. In general, k1 should be the immediate successor of k
     * in the natural value space for k.
     * 
     * @param k
     *            The coerced key (eg, Long, Integer, etc).
     * @param k1
     *            The successor of the coerced key in its natural value space
     *            (an instance of the same class).
     */
    private void doSuccessorTest(Object k, Object k1) {

        assertEquals(k.getClass(), k1.getClass());
        
        final byte[] key = KeyBuilder.asSortKey(k); // key for v.
        
        final byte[] successor = KeyBuilder.asSortKey(k1); // key for v1.
        
        assertTrue(BytesUtil.compareBytes(key,successor) < 0);

    }
    
    /**
     * Test successor semantics for composite keys (indices that allow duplicate
     * keys). Keys are not coerced except for untyped property values, which are
     * coerced using generic type conversion to a String. The successor of a key
     * is always formed in the natual value space of the (coerced) key, so this
     * is the natural value space of the property value except for untyped
     * property values since those are coerced to Strings. The composite key is
     * then formed by converting the (coerced) key to an unsigned byte[] and
     * appending the oid to that unsigned byte[].
     * 
     * @todo test with other value types and especially with String.
     * 
     * @todo write direct tests of getInternalKey(...)?
     */
    public void test_successor_compositeKey() {

        doSuccessorCompositeKeyTest(0L, 1L, 12L/*oid*/);
        
        doSuccessorCompositeKeyTest(-1L, 0L, 12L/*oid*/);

        doSuccessorCompositeKeyTest('a', 'b', 12L/*oid*/);

        doSuccessorCompositeKeyTest("", "\0", 12L/*oid*/);

        doSuccessorCompositeKeyTest("abc", "abc\0", 12L/*oid*/);
           
    }
    
    private void doSuccessorCompositeKeyTest(Object k, Object k1, long oid) {
        
        final byte[] key = KeyBuilder.asSortKey(k); // sort key for k.
        
        final byte[] key1 = KeyBuilder.asSortKey(k1); // sort key for successor(k).
        
        // sort keys are in the correct order.
        assertTrue(BytesUtil.compareBytes(key, key1) < 0);

        final byte[] compositeKey = MyBTree.newCompositeKey(key,oid); // composite key for k
        
        final byte[] compositeKey1 = MyBTree.newCompositeKey(key1,oid); // composite key for successor(k).

        // composite sort keys are in the correct order.
        assertTrue(BytesUtil.compareBytes(compositeKey, compositeKey1) < 0);
        
    }
  
    /*
     * the code below was based on one set of assumptions about how to form
     * keys for the index.
     */
    
//    /**
//     * Test successor semantics.
//     */
//    public void test_successor() {
//        
//        /*
//         * long
//         */
//        {
//            
//            doSuccessorTest( 0L );
//            
//            doSuccessorTest( -1L );
//            
//            doSuccessorTest( 1L );
//            
//            doSuccessorTest( 12L );
//            
//            doSuccessorTest( -12L );
//            
//        }
//        
//    }
//
//    /**
//     * Test successor semantics for a long integer.
//     * <p>
//     * Note: The test verifies that the key generated by
//     * {@link BytesUtil#successor(byte[])} is less than the key generated from
//     * <code>v + 1</code> when the keys are compared as unsigned byte arrays.
//     * This is true because, while <code>v + 1</code> is the next value within
//     * an 8 byte value space, the next value for an variable length unsigned
//     * byte[] is always formed by appending an unsigned zero byte.
//     * 
//     * @param v
//     *            The long integer.
//     */
//    private void doSuccessorTest(final long v) {
//        
//        final long v1 = v + 1; // successor of v.
//        
//        final byte[] k = KeyBuilder.asSortKey(v); // key for v.
//        
//        final byte[] k1 = KeyBuilder.asSortKey(v1); // key for v1.
//        
//        assertTrue(BytesUtil.compareBytes(k, k1)<0); // k LT k1.
//        
//        final byte[] successor = BytesUtil.successor(k);
//        
//        assertTrue(BytesUtil.compareBytes(successor, k1) < 0);
//        
//    }
//    
//    /**
//     * Test successor semantics for a key when made into a composite key for a
//     * {@link LinkSetIndex} that supports duplicate keys. In particular, this
//     * test verifies keys used by {@link LinkSetIndex#getPoints(Object)}.
//     * <p>
//     * Note: The {@link Coercer} for any strongly typed property value (other
//     * than a byte[]) is {@link DefaultUnsignedByteArrayCoercer}. If the
//     * property value is not strongly type then the coercer is
//     * {@link GenericUnsignedByteArrayCoercer}. A property value that is
//     * strongly typed as a byte[] is NOT coerced.
//     * 
//     * @throws NoSuccessorException 
//     * 
//     * @todo also test for byte[] and untyped (or string) properties
//     * 
//     * @todo check at overflow points.
//     */
//    public void test_successor_compositeKey() throws NoSuccessorException {
//
//        doSuccessorCompositeKeyTest(Long.valueOf(0));
//        doSuccessorCompositeKeyTest(Long.valueOf(1));
//        doSuccessorCompositeKeyTest(Long.valueOf(Long.MAX_VALUE));
//        doSuccessorCompositeKeyTest(Long.valueOf(Long.MIN_VALUE));
//        doSuccessorCompositeKeyTest(Long.valueOf(-1));
//
//        doSuccessorCompositeKeyTest(Integer.valueOf(0));
//
//    }
//    
//    private void doSuccessorCompositeKeyTest(Object key) throws NoSuccessorException {
//        
//        // key coerced from value.
//        final byte[] k = KeyBuilder.asSortKey(key);
//        
//        // successor of the coerced key.
////        final byte[] k1 = BytesUtil.successor(k);
//        final byte[] k1 = _successor(k);
//        
//        // internal key for the coerced value.
//        final byte[] i = MyBTree.newCompositeKey(k, 0L/*oid*/);
//
//        // internal key for successor of the coerced value.
//        final byte[] i1 = MyBTree.newCompositeKey(k1, 0L/*oid*/);
//        
//        assertTrue(BytesUtil.compareBytes(k, k1)<0);
//
//        /*
//         * FIXME This case fails. It winds up comparing the bytes -128 (in i,
//         * which is part of the oid appended to k) and 0 (in i1, which is the
//         * byte appended to k to form its successor k1). This fails of course
//         * since -128 LT 0.
//         * 
//         * It appears that we need to compute the successor in the original
//         * value space for the key. E.g., by incrementing the low bit with
//         * rollover and rejecting cases that would overflow the high bit. This
//         * is easily done in the original value space, but I am less sure how to
//         * perform this operation on a byte[].
//         * 
//         * @todo verify that this operation is correct when the keys are strings
//         * since their variable length nature may play havok with us again.
//         */
//        assertTrue(BytesUtil.compareBytes(i, i1)<0);
//
//    }
//    
//    private byte[] _successor(byte[] k) throws NoSuccessorException {
//        
//        BigInteger i = new BigInteger(k);
//        
//        BigInteger i1 = i.add(BigInteger.ONE);
//        
//        if(i1.bitCount()>i.bitCount()) throw new NoSuccessorException();
////        if(i1.bitLength()>i.bitLength()) throw new NoSuccessorException();
//        
//        return i1.toByteArray();
//        
//    }
    
}
