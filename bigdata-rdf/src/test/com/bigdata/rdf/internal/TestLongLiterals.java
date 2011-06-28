/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on May 4, 2010
 */

package com.bigdata.rdf.internal;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import junit.framework.TestCase2;

/**
 * This class contains some code which was written to see which hash functions
 * were available on the JVM. The idea was to identify a hash function which has
 * a low probability of collisions. E.g., MD5 or better (MD5 can be spoofed
 * another cryptographic hash function might be better).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This can be deleted when we finish the TERMS_REFACTOR.
 */
public class TestLongLiterals extends TestCase2 {

    /**
     * 
     */
    public TestLongLiterals() {
    }

    /**
     * @param name
     */
    public TestLongLiterals(String name) {
        super(name);
    }

    /**
     * <code>MD2</code> is the MD2 message digest algorithm as defined in <a
     * href="http://www.ietf.org/rfc/rfc1319.txt"RFC 1319</a>.
     * 
     * @throws NoSuchAlgorithmException
     */
    public void test_availableMessageDigestAlgorithms_MD2()
            throws NoSuchAlgorithmException {

        MessageDigest.getInstance("MD2");

    }

    /**
     * <code>MD5</code> is the MD5 message digest algorithm as defined in <a
     * href="http://www.ietf.org/rfc/rfc1321.txt">RFC 1321</a>. Note that MD5
     * has (relatively) weak collision resistance. For example, see <a
     * href="http://www.cs.cmu.edu/~perspectives/md5.html">MD5 and
     * Perspectives</a>, <a
     * href="http://en.wikipedia.org/wiki/Cryptographic_hash_function"
     * >Wikipedia</a> or http://eprint.iacr.org/2009/223.pdf
     * 
     * @throws NoSuchAlgorithmException
     */
    public void test_availableMessageDigestAlgorithms_MD5()
            throws NoSuchAlgorithmException {

        MessageDigest.getInstance("MD5"); // weak collision resistance.

    }

    /**
     * Appropriate hash algorithms are defined in the <a
     * href="http://csrc.nist.gov/publications/fips/index.html">FIPS PUB
     * 180-2</a> (which has been replaced by <a href=
     * "http://csrc.nist.gov/publications/fips/fips180-3/fips180-3_final.pdf"
     * >FIPS PUB 180-3</a>. Also see Recommendation for Applications Using
     * Approved Hash Algorithms in <a href=
     * "http://csrc.nist.gov/publications/nistpubs/800-107/NIST-SP-800-107.pdf"
     * >SP 800-107</a>, which provides information about the collision
     * resistance of these hash algorithms.
     */
    public void test_availableMessageDigestAlgorithms_SHA()
            throws NoSuchAlgorithmException {

        MessageDigest.getInstance("SHA-1"); // 160 bits (20 bytes, not a good choice).
        MessageDigest.getInstance("SHA-256"); // 256 bits (32 bytes)
        MessageDigest.getInstance("SHA-384"); // 384 bits (48 bytes)
        MessageDigest.getInstance("SHA-512"); // 512 bits (64 bytes)

    }

//    public void test_more() {
//        
//        fail("Integration test with long literals");
//        
//    }
    
}
