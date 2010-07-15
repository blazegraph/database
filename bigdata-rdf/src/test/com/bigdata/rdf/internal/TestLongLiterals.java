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

import com.bigdata.bfs.BigdataFileSystem;
import com.bigdata.btree.IOverflowHandler;

/**
 * Above a configured size, long literals are transparently promoted to blobs.
 * When a long literal is recognized, a hash code is computed from the literal
 * using the configured cryptographic hash function. That hash code serves as
 * the key in the TERM2ID index. The TERM2ID index assigns a term identifier,
 * just as it always does. The ID2TERM index stores the mapping from the term
 * identifier onto a blob reference for the long literal. The blob reference is
 * stored on the journal associated with the ID2TERM index / shard. On overflow,
 * the blob is copied into an index segment as part of the shard build and the
 * blob reference is updated by the {@link IOverflowHandler} so that it will now
 * resolve the blob against the index segment rather than the journal.
 * 
 * FIXME Test long literals.
 * 
 * @todo It would be nice to send the hash code rather than the literal to the
 *       TERM2ID index to avoid the overhead of sending the long literal to both
 *       the TERM2ID and ID2TERM indices.
 * 
 * @todo Above some size we need to use a hash function of the literal as the
 *       key. The literal itself will be stored in a raw record on the journal.
 *       The TERM2ID index would map the hash code of the literal onto a blob
 *       reference. The blob reference will be linked to the TERM2ID shard and
 *       will migrate naturally to an index segment on overflow. The ID2TERM
 *       index will not store the reverse mapping. Instead, the blob reference
 *       is de-referenced against the TERM2ID index. The blob reference can
 *       appear inline in the statement indices [this suggests that we need to
 *       extend from one byte to two bytes for the header.]
 *       <p>
 *       The blob reference will need the identity of the index partition
 *       against which it must be resolved. The easiest way to do this is to
 *       encode the hash code of the literal into the blob reference together
 *       with the address of the raw record for that blob. No, that does not
 *       work since the raw address needs to be updated during overflow. So we
 *       have to generate a term identifier for the literal, resolve the term
 *       identifier to a blob reference using ID2TERM, and store the raw record
 *       with the ID2TERm shards.
 *       <p>
 *       See the {@link BigdataFileSystem} for a blob reference implementation.
 *       <p>
 *       We need to use a hash function which has a low probability of
 *       collisions. E.g., MD5 or better (MD5 can be spoofed another
 *       cryptographic hash function might be better).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

    public void test_more() {
        
        fail("Integration test with long literals");
        
    }
    
}
