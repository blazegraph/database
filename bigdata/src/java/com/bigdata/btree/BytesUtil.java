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
package com.bigdata.btree;

import java.util.Comparator;

import org.apache.log4j.Logger;

/**
 * Class supporting operations on variable length byte[] keys.
 * <p>
 * Comparison operations that accept a starting offset are used when the byte[]s
 * are known to share a leading prefix that may be skipped during comparison.
 * <p>
 * Comparison operations that accept a starting offset and length are used when
 * immutable keys are stored in a single byte[] and an index into starting
 * positions in that byte[] is maintained.
 * <p>
 * JNI methods are provided for unsigned byte[] comparison. However, note that
 * the JNI methods do not appear to be as fast as the pure Java methods -
 * presumably because of the overhead of going from Java to C. In order to
 * execute using the JNI methods you MUST define the optional boolean system
 * property, e.g.,
 * 
 * <pre>
 * java -Dcom.bigdata.btree.BytesUtil.jni=true ...
 * </pre>
 * 
 * See BytesUtil.c in this package for instructions on compiling the JNI
 * methods.
 * </p>
 * See {@link #main(String[])} which provides a test for the JNI integration and
 * some pointers on how to get this running on your platform.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BytesUtil {

    protected static final Logger log = Logger.getLogger(BytesUtil.class);

    /**
     * Flag set iff JNI linking succeeds.  When this flag is false we run with
     * the pure Java implementations of these methods.  When the flag is true,
     * the JNI versions are used.
     */
    static boolean linked = false;

    /**
     * JNI routines are not invoked unless we will compare byte[]s with at least
     * this many potential bytes to compare (the actual# may be much less of
     * course since comparisons may fail fast).
     */
    static public final int minlen = 100;
    
    static private native int _compareBytes(int alen, byte[] a, int blen, byte[] b);
    static private native int _compareBytesWithOffsetAndLen(int aoff, int alen, byte[] a, int boff, int blen, byte[] b);

    static {

        final boolean jni;

        String val = System.getProperty("com.bigdata.btree.BytesUtil.jni");

        if (val != null) {

            jni = Boolean.parseBoolean(val);

        } else {

            jni = false; // Note: We will not even try to use JNI by default!

        }

        if (jni) {

            /*
             * Attempt to load the JNI library.
             */
            
            loadJNILibrary();
            
        }
        
    }

    /**
     * Attempt to load the JNI library.
     * <p>
     * Note: this is done automatically if the optional boolean system property
     * <code>com.bigdata.btree.BytesUtil.jni=true</code> is specified, e.g.,
     * using
     * 
     * <pre>
     *    java -Dcom.bigdata.btree.BytesUtil.jni=true ...
     * </pre>
     * 
     * @return True iff the JNI library was successfully linked.
     */
    public static boolean loadJNILibrary() {

        if (!linked) {
            
            try {

                System.loadLibrary("BytesUtil");

                log.info("BytesUtil JNI linked");

                linked = true;

            } catch (UnsatisfiedLinkError ex) {

                log.warn("BytesUtil JNI NOT linked: " + ex);

                linked = false;

            }
        }

        return linked;

    }
    
    /**
     * True iff the two arrays compare as equal. This is somewhat optimized in
     * that it tests the array lengths first, assumes that it is being used on
     * sorted data and therefore compares the last bytes first, and does not
     * convert the bytes to unsigned integers before testing for equality.
     * 
     * @param a
     *            A byte[].
     * @param b
     *            Another byte[].
     * 
     * @return If the two arrays have the same data.
     */
    final public static boolean bytesEqual(final byte[] a, final byte[] b) {

        final int alen = a.length;

        final int blen = b.length;

        if (alen != blen)
            return false;
        
        int i = alen-1;

        while(i>=0) {
        
            if( a[i] != b[i] ) return false;
            
            i--;
            
        }

//        for (int i = 0; i < alen; i++) {
//        
//            if( a[i] != b[i] ) return false;
//
//        }
        
        return true;
        
    }
    
    /**
     * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
     * unsigned bytes).
     * 
     * @param a
     *            A byte[].
     * 
     * @param b
     *            A byte[].
     * 
     * @return a negative integer, zero, or a positive integer as the first
     *         argument is less than, equal to, or greater than the second.
     * 
     * @todo consider returning the index of the byte at which the difference
     *       was detected rather than the difference of the bytes at that index.
     *       the index would be negative or positive depending on which way the
     *       comparison went.
     */
    final public static int compareBytes(final byte[] a, final byte[] b) {
        final int alen = a.length;
        final int blen = b.length;
        if (linked && alen > minlen && blen > minlen) {
            /*
             * JNI implementation.
             * 
             * @todo test for trade off when max(len) is short. unroll loop for
             * small N.
             */
            return _compareBytes(alen,a, blen,b);
        }
        for (int i = 0; i < alen && i < blen; i++) {
            // promotes to signed integers in [0:255] for comaprison.
            int ret = (a[i] & 0xff) - (b[i] & 0xff);
            //                int ret = a[i] - b[i];
            if (ret != 0)
                return ret;
        }
        return alen - blen;
    }

//    /**
//     * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
//     * unsigned bytes).
//     * 
//     * @param aoff
//     *            The offset into <i>a</i> at which the comparison will
//     *            begin.
//     * @param a
//     *            A byte[].
//     * @param boff
//     *            The offset into <i>b</i> at which the comparison will
//     *            begin.
//     * @param b
//     *            A byte[].
//     * 
//     * @return a negative integer, zero, or a positive integer as the first
//     *         argument is less than, equal to, or greater than the second.
//     */
//    final public static int compareBytes(int aoff, final byte[] a, int boff,
//            final byte[] b) {
//        final int alen = a.length;
//        final int blen = b.length;
//        for (int i = aoff, j = boff; i < alen && j < blen; i++, j++) {
//            // promotes to signed integers in [0:255] for comaprison.
//            int ret = (a[i] & 0xff) - (b[j] & 0xff);
//            //                int ret = a[i] - b[j];
//            if (ret != 0)
//                return ret;
//        }
//        return (alen - aoff) - (blen - boff);
//    }

    /**
     * Byte-wise comparison of byte[]s (the arrays are treated as arrays of
     * unsigned bytes).
     * 
     * @param aoff
     *            The offset into <i>a</i> at which the comparison will
     *            begin.
     * @param alen
     *            The #of bytes in <i>a</i> to consider starting at <i>aoff</i>.
     * @param a
     *            A byte[].
     * @param boff
     *            The offset into <i>b</i> at which the comparison will
     *            begin.
     * @param blen
     *            The #of bytes in <i>b</i> to consider starting at <i>boff</i>.
     * @param b
     *            A byte[].
     * 
     * @return a negative integer, zero, or a positive integer as the first
     *         argument is less than, equal to, or greater than the second.
     */
    final public static int compareBytesWithLenAndOffset(//
            int aoff, int alen, final byte[] a,//
            int boff, int blen, final byte[] b//
    ) {
        
        if (linked && alen > minlen && blen > minlen) {
            
            // JNI implementation.
            return _compareBytesWithOffsetAndLen(aoff, alen, a, boff, blen, b);
            
        }

        // last index to consider in a[].
        final int alimit = aoff + alen;

        // last index to consider in b[].
        final int blimit = boff + blen;
        
        for (int i = aoff, j = boff; i < alimit && j < blimit; i++, j++) {
            
            // promotes to signed integers in [0:255] for comaprison.
            int ret = (a[i] & 0xff) - (b[j] & 0xff);
            
            if (ret != 0)
                return ret;
        
        }
        
        return alen - blen;
        
    }

    /**
     * Return the #of leading bytes in common. This is used to compute the
     * prefix for a node or leaf, which is formed by the leading bytes in common
     * between the first and last key for a node or leaf.
     * 
     * @param a
     *            A variable length unsigned byte array.
     * @param b
     *            A variable length unsigned byte array.
     * 
     * @return The #of leading bytes in common (aka the index of the first byte
     *         in which the two arrays differ, although that index could lie
     *         beyond the end of one of the arrays).
     */
    public final static int getPrefixLength(byte[] a, byte[] b) {

        final int alen = a.length;

        final int blen = b.length;

        int i;

        for (i = 0; i < alen && i < blen; i++) {

            if (a[i] != b[i])
                break;

        }

        return i;

    }

    /**
     * Return a new byte[] containing the leading bytes in common between two
     * byte[]s. This is often used to compute the minimum length separator key.
     * 
     * @param a
     *            A variable length unsigned byte array[].
     * @param b
     *            A variable length unsigned byte array[].
     * 
     * @return A new byte[] containing the leading bytes in common between the
     *         two arrays.
     */
    public final static byte[] getPrefix(byte[] a, byte[] b) {

        final int len = getPrefixLength(a, b);

        byte[] prefix = new byte[len];
        
        System.arraycopy(a, 0, prefix, 0, len);
        
        return prefix;
        
    }
    
    /**
     * Computes the successor of a variable length byte array by appending
     * a zero(0) byte to the end of the array.
     * 
     * @param key
     *            A variable length unsigned byte array.
     * 
     * @return A new byte[] that is the successor of the key.
     */
    public final static byte[] successor(byte[] key) {

        final int keylen = key.length;

        byte[] tmp = new byte[keylen + 1];

        System.arraycopy(key, 0, tmp, 0, keylen);

        return tmp;

    }

    /**
     * <p>
     * The keys in the nodes of a btree are known as <i>separator keys</i>. The
     * role of the separator keys is to direct search towards the leaf in which
     * a key exists or would exist by always searching the first child having a
     * separator key that is greater than or equal to the search key.
     * </p>
     * <p>
     * Separator keys separate leaves and must be choosen with that purpose in
     * mind. The simplest way to choose the separator key is to just take the
     * first key of the leaf - this is always correct. However, shorter
     * separator keys may be choosen by defining the separator key as the
     * shortest key that is less than or equal to the first key of a leaf and
     * greater than the last key of the left sibling of that leaf (that is, the
     * key for the entry that immediately proceeds the first entry on the leaf).
     * </p>
     * <p>
     * There are several advantages to always choosing the shortest separator
     * key. The original rationale (in "Prefix <i>B</i>-Trees" by Bayer and
     * Unterauer) was to increase the branching factors for fixed size pages.
     * Since we use variable size serialized record, that is not an issue.
     * However, using the shortest separator keys in this implementation
     * provides both smaller serialized records for nodes and faster search
     * since fewer bytes must be tested.
     * </p>
     * <p>
     * Note that this trick can not be used at higher levels in the btree -
     * separator keys are always formed based on the keys in the leaves and then
     * propagated through the tree.
     * </p>
     * <p>
     * There are several cases to consider:
     * <ol>
     * <li>When the two keys differ only in the last byte of the <i>givenKey</i>
     * then we return the <i>givenKey</i> since no shorter separator key
     * exists.</li>
     * <li>Otherwise the prefix MUST be at least two bytes shorter than the
     * givenKey. If the givenKey[prefixLen] - priorKey[prefixLen] GT one (1)
     * then the separator key is formed by adding one to the last byte in the
     * prefix.</li>
     * <li>Otherwise the separator key is formed by appending a nul byte to the
     * prefix.</li>
     * </ol>
     * </p>
     * 
     * @param givenKey
     *            A key.
     * 
     * @param priorKey
     *            Another key that <em>proceeds</em> the <i>givenKey</i>.
     * 
     * @return The shortest key that is less than or equal to <i>givenKey</i>
     *         and greater than <i>priorKey</i>. This will be a reference to
     *         the <i>givenKey</i> iff that is also the shortest separator.
     * 
     * @see http://portal.acm.org/citation.cfm?doid=320521.320530
     * 
     * FIXME implement shortest key logic. this returns the givenKey, which is
     * always legal.
     */
    final public static byte[] getSeparatorKey(byte[] givenKey, byte[] priorKey) {

        return givenKey;
        
//        final int givenKeyLen = givenKey.length;
//
//        final int priorKeyLen = priorKey.length;
//
//        int i;
//
//        for (i = 0; i < givenKeyLen && i < priorKeyLen; i++) {
//
//            // promotes to signed integers in [0:255] for comaprison.
//            int ret = (givenKey[i] & 0xff) - (priorKey[i] & 0xff);
//            
//            if (ret != 0) {
//                
//                throw new UnsupportedOperationException();
//
//            }
//
//        }
//        
//        if( priorKey.length > i ) {
//            
//            throw new IllegalArgumentException("keys out of order.");
//            
//        }
//
//        throw new UnsupportedOperationException();
        
    }
    
//    final public static byte[] getSeparatorKey(byte[] givenKey, byte[] priorKey) {
//        
//        /*
//         * error checking.
//         */
//        
//        if( givenKey == null || priorKey == null ) {
//            
//            throw new IllegalArgumentException("null key");
//            
//        }
//        
//        if( givenKey == priorKey ) {
//            
//            throw new IllegalArgumentException("same key object");
//            
//        }
//        
//        /* #of leading bytes in common.
//         * 
//         * given [2,3]   and [1,2]   ==> 0
//         * given [1,2,3] and [1,2]   ==> 2
//         * given [1,2,3] and [1,3]   ==> 1
//         * given [1,2,3] and [0,2]   ==> 0
//         * given [1,2,3] and [1,2,2] ==> 2
//         */
//        final int prefixLength = getPrefixLength(givenKey, priorKey);
//
//        /*
//         * more error checking.
//         */
//        
//        /*
//         * If both arrays define the byte at position prefixLength then it MUST
//         * be true that givenKey[prefixLength] GT priorKey[prefixLength]
//         * otherwise the parameters are out of order.
//         * 
//         * Otherwise it MUST be true that priorKey.length LT givenKey.length.
//         * 
//         * given [1,2,3] and [1,2]    , prefixLength=2 -- ok.
//         * given [1,2,3] and [1,2,2]  , prefixLength=2 -- ok.
//         * given [1,2,3] and [1,2,2,3], prefixLength=2 -- ok.
//         * given [1,2,3] and [1,2,3]  , prefixLength=3 -- keys out of order.
//         * given [1,2,3] and [1,2,4]  , prefixLength=2 -- keys out of order.
//         * given [1,3]   and [1,2,4]  , prefixLength=1 -- ok.
//         */
//        
//        if( priorKey.length > prefixLength && givenKey.length > prefixLength ) {
//            
//            if( (givenKey[prefixLength] & 0xff) <= (priorKey[prefixLength] & 0xff) ) {
//                
//                throw new IllegalArgumentException("parameters are out of order");
//                
//            }
//            
//        } else {
//            
//            if( priorKey.length >= givenKey.length ) {
//                
//                throw new IllegalArgumentException("parameters are out of order");
//                
//            }
//            
//        }
//
//        if( prefixLength == givenKey.length - 1 ) {
//
//            /*
//             * two keys differ only in the last byte of the givenKey so
//             * separator key exists that is shorter than the given key.
//             * 
//             * given [1,2,3] and [1,2,2] => [1,2,3]
//             * 
//             * This also handles cases when the priorKey is shorter than the
//             * given key by one byte and the entire priorKey forms the prefix.
//             * 
//             * given [1,2,3] and [1,2] => [1,2,3]
//             * 
//             * This does NOT handle cases where the prefix is less than the
//             * length of the givenKey by two or more bytes.
//             */
//            
//            return givenKey;
//            
//        }
//
//        /*
//         * Since the two keys do not differ in the last byte of the givenKey the
//         * prefix MUST be at least two bytes shorter than the givenKey.
//         * 
//         * If the givenKey[prefixLen] - priorKey[prefixLen] GT one (1) then the
//         * separator key is formed by adding one to the last byte in the prefix.
//         * 
//         * given [1,2,3] and [1] => [1,0]
//         * given [2,2,3] and [1] => [1,0]
//         */
//        
//        if( priorKey.length >= prefixLength ) {
//            
//            // priorKey[prefixLength] is defined.
//            
//            int diff = (givenKey[prefixLength] & 0xff) - (priorKey[prefixLength] & 0xff);
//            
//            if( diff > 1 ) {
//                
//                byte[] separatorKey = new byte[prefixLength];
//                
//                System.arraycopy(givenKey, 0, separatorKey, 0, prefixLength);
//                
//                separatorKey[prefixLength]++; // @todo validate unsigned math.
//                
//                return separatorKey;                
//                
//            }
//            
//        }
//
//        /*
//         * 
//         * Otherwise the separator key is formed by appending a nul byte to the
//         * prefix.
//         * 
//         * given [3,2,3] and [1] => [2]
//         * 
//         * allocate one extra byte which will remain zero and hence form the
//         * successor of the prefix.
//         */
//
//        byte[] separatorKey = new byte[prefixLength + 1];
//        
//        System.arraycopy(givenKey, 0, separatorKey, 0, prefixLength);
//        
//        return separatorKey;
//
//    }
    
    /**
     * Formats a key as a series of comma delimited unsigned bytes.
     * 
     * @param key
     *            The key.
     * 
     * @return The string representation of the array as unsigned bytes.
     */
    final public static String toString(byte[] key) {

        StringBuilder sb = new StringBuilder(key.length*4+2);
        
        sb.append("[");
        
        for( int i=0; i<key.length; i++) {
            
            if( i>0 ) sb.append(", ");
            
            // as an unsigned integer.
//            sb.append(Integer.toHexString(key[i] & 0xff));
            sb.append(Integer.toString(key[i] & 0xff));
            
        }
        
        sb.append("]");
        
        return sb.toString();
        
    }
    
    /**
     * Compares two unsigned byte[]s.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class UnsignedByteArrayComparator implements Comparator<byte[]> {

        public static transient final Comparator<byte[]> INSTANCE = new UnsignedByteArrayComparator();
        
        public int compare(byte[] o1, byte[] o2) {
            
            return BytesUtil.compareBytes(o1, o2);
            
        }
        
    }
    
    /**
     * This method forces the load of the JNI library and tries to execute the
     * JNI methods.
     * <p>
     * In order to use the JNI library under Windows, you must specify the JNI
     * library location using the PATH environment variable, e.g.,
     * 
     * <pre>
     *   cd bigdata
     *   set PATH=%PATH%;lib
     *   java -cp bin com.bigdata.btree.BytesUtil
     * </pre>
     * 
     * <p>
     * In order to use the JNI library under un*x, you must specify the JNI
     * library location
     * 
     * <pre>
     *    java -Djava.library.path=lib com.bigdata.btree.BytesUtil
     * </pre>
     * 
     * @param args
     * 
     * @exception UnsatisfiedLinkError
     *                if the JNI methods can not be resolved.
     * @exception AssertionError
     *                if the JNI methods do not produce the expected answers.
     */
    public static void main(String[] args) {
 
        // Force load of the JNI library.
        loadJNILibrary();
        
        if( 0 != BytesUtil._compareBytes(3, new byte[]{1,2,3}, 3, new byte[]{1,2,3}) ) {
            
            throw new AssertionError();
            
        }

        if( 0 != BytesUtil._compareBytesWithOffsetAndLen(0, 3, new byte[]{1,2,3}, 0, 3, new byte[]{1,2,3}) ) {

            throw new AssertionError();
            
        }

        System.err.println("JNI library routines Ok.");
        
    }
    
}
