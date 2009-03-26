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
 * Created on Mar 24, 2009
 */

package com.bigdata.rdf.lexicon;

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Utility class handles the low-level details of encoding a term identifier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermIdEncoder {

    /**
     * When set to debug, this will log the gruesome details of the transform.
     * Normally you would only do this in combination with the test suite. When
     * set to info, this will apply asserts to verify that the transform is
     * reversable.
     * 
     * @see TestTermIdEncoder
     */
    static protected final Logger log = Logger.getLogger(TermIdEncoder.class);

    static protected final boolean INFO = log.isInfoEnabled();

    static protected final boolean DEBUG = log.isDebugEnabled();

    /**
     * The maximum value for a localCounter which can be used with the scale-out
     * encoding given that we will steal 2 bits for bit flags encoding whether
     * the term was a URI, Literal BNode, or Statement.
     */
    static protected final int maxLocalCounter = (Integer.MAX_VALUE >> 1);

    /**
     * Assign a term identifier. The result is never negative (since the term
     * identifiers are packed by a routine which requires non-negative values)
     * and never ZERO (since ZERO is reserved to indicate a
     * {@link IRawTripleStore#NULL}).
     * <p>
     * The low two bits are flags which indicate whether a term is a
     * {@link Literal}, {@link BNode}, {@link URI}, or {@link Statement} so
     * that we can tell at a glance (without lookup up the term in the index)
     * what "kind" of thing the term identifier stands for. You can test those
     * flags using {@link AbstractTripleStore#isURI(long)} and friends.
     * <p>
     * Note: A different encoding is used for the scale-out archicture which is
     * designed to distribute tuples in the ID2TERM and statement indices (SPO,
     * POS, and OSP) more uniformly. This is achieved by reversing the bits from
     * the localCounter value and placing them in the high-word of the generated
     * term identifier.
     * 
     * @param scaleOut
     *            <code>true</code> iff the index is logically partitioned.
     * @param counter
     *            The counter value assigned by the index or index partition.
     * @param code
     *            The byte code indicating the type of the RDF {@link Value}
     *            whose term identifier is being encoded.  The valid byte codes
     *            are defined by {@link ITermIndexCodes}. 
     *            
     * @todo This scheme is NOT used for the scale-up architecture since it
     *       tends to distribute the keys uniformly and thus defeats the BTree
     *       cache to some extent.
     * 
     * FIXME Check performance for scheme 2 on the unpartitioned case. If
     * performance suffers there then we should have a scheme specifically
     * designed for unpartitioned indices, which would mean that index segments
     * generated from a triple store on an unpartitioned Journal would not be
     * binary compatible with those generated from a key-range partitioned
     * federation. E.g., you have to export as RDF/XML and import the data.
     */
    static public long encode(final boolean scaleOut, final long counter,
            final byte code) {

        if (counter == IRawTripleStore.NULL) {

            /*
             * 0L is never used as a counter value and is not permitted in a
             * term identifier since it can be mapped to a NULL (0L) when the
             * term is a URI (the bit flags are zero for a URI).
             */
            
            throw new IllegalArgumentException();
            
        }

        if (false && scaleOut) {

            // the index partition local counter.
            final int localCounter = (int) (counter >> 0) & 0xffffffff;

            // the index partition identifier.
            final int partitionId = (int) (counter >> 32) & 0xffffffff;

            return encodeScaleOut(partitionId, localCounter, code);

        }

        return encodeScaleUp(counter, code);

    }

    /**
     * Scheme does not rotate bytes around so the low bytes vary more quickly
     * than the high bytes. The 2 low bits are the bit flags indicating the type
     * of RDF {@link Value}. The #of allowable index partition is reduced by 4
     * since those bits are stolen from the high order bits of the partition
     * identifier.
     * 
     * @param v
     *            The index partition local counter value.
     * @param code
     *            The code indicating the type of RDF {@link Value}.
     * 
     * @return The term identifier.
     */
    static public long encodeScaleUp(final long v, final byte code) {

        // Left shift two bits to make room for term type coding.
        long id = v << 2;

        /*
         * Set the bit flags for the Value type on the 2 low order bits.
         */
        switch (code) {

        case ITermIndexCodes.TERM_CODE_URI:

            id |= ITermIdCodes.TERMID_CODE_URI;

            break;

        case ITermIndexCodes.TERM_CODE_LIT:
        case ITermIndexCodes.TERM_CODE_DTL:
        case ITermIndexCodes.TERM_CODE_LCL:

            id |= ITermIdCodes.TERMID_CODE_LITERAL;

            break;

        case ITermIndexCodes.TERM_CODE_BND:

            id |= ITermIdCodes.TERMID_CODE_BNODE;

            break;

        case ITermIndexCodes.TERM_CODE_STMT:

            id |= ITermIdCodes.TERMID_CODE_STATEMENT;

            break;

        default:

            throw new AssertionError("Unknown term type: code=" + code);

        }

        return id;

    }

    /**
     * This encoding is designed to support the scale-out architecture and is
     * explicitly aware of the index partition (a 32-bit value) and the local
     * index partition counter (a 32-bit value). In order to avoid hot spots on
     * the ID2TERM and the statement indices (SPO, POS, OSP), this encoding
     * reverses the bits in the localCounter and places them into the high int32
     * word and places the index partition identifier into the low int32 word
     * (it actually extends 1 bit into the high int32 word) along with the bit
     * flags for the RDF Value type (the low 2 bits). This encoding causes the
     * high-bits vary more quickly than the low bits regardless of the index
     * partition and causes the assigned term identifiers to be evenly
     * distributed across the index partitions regardless of the #of index
     * partitions. The #of allowable localCounter values is reduced by 4 since
     * those bits are stolen to encode the bit flags. In practice, this is not a
     * problem for the scale-out architecture since index partitions are split
     * when they have a few 100 MB of data and therefore the localCounter will
     * not approach {@link Integer#MAX_VALUE}.
     * 
     * <h3>Approach</h3>
     * 
     * <ol>
     * 
     * <li>The low 2 bits of the resulting term identifier must be bit flags
     * encoding whether the term is a URI, Literal, BNode or statement
     * identifier.</li>
     * 
     * <li>The high bit must be zero because we are packing the long term
     * identifiers using a method which requires non-negative values.</li>
     * 
     * <li>The high bits need to vary quickly. Therefore we use the
     * localCounter in the high word since it is what varies and we reverse the
     * bits in the localCounter using {@link Integer#reverse(int)} so that the
     * bits which vary the most quickly wind up in the highest bit positions.</li>
     * 
     * <li>We steal 2 bits from the local counter for the RDF value type bit
     * flags rather than from the partition identifier since an index partition
     * will NEVER have 32-bits worth of tuples (because we split them in order
     * to keep their size on disk managable).</li>
     * 
     * <li>Since we want to steal 2 bits from the local counter, those are
     * going to be the bits in the high bits before the swap and the low bits
     * after the swap.</li>
     * 
     * <li>We extend the partitionId to a long, shift it left by two bits (to
     * make room for the bit flags), and OR it with the long value. This
     * preserves all (non-sign) bits from the partition identifier while leaving
     * the lowest 2 bits of the long empty. Since the sign bit of the partition
     * identifier is zero we are not going to stomp on the low bit of the
     * swapped localCounter when we OR these quantities together.</li>
     * 
     * <li>OR the long value with the bit flags indicating whether the term is
     * a URI, Literal, BNode or Statement identifier into the low 2 bits of the
     * long.</li>
     * 
     * <li>Return that long value.</li>
     * 
     * </ol>
     * 
     * @param partitionId
     *            The index partition identifier.
     * @param localCounter
     *            The local counter value assigned by that index partition.
     * @param code
     *            The code indicating the type of RDF {@link Value}.
     * 
     * @return The term identifier.
     */
    static public long encodeScaleOut(final int partitionId,
            final int localCounter, final byte code) {

        if (partitionId < 0)
            throw new IllegalArgumentException("partitionId LT ZERO: "
                    + partitionId);

        if (localCounter <= 0)
            throw new IllegalArgumentException("localCounter LTE ZERO: "
                    + localCounter);

        if (localCounter > maxLocalCounter)
            throw new IllegalArgumentException("localCounter=" + localCounter
                    + ", but maxLocalCounter=" + maxLocalCounter + //
                    "\n" + Integer.toBinaryString(localCounter) + " : given" + //
                    "\n" + Integer.toBinaryString(maxLocalCounter) + " : max"//
            );

        /*
         * Reverse the bits in the localCounter.
         * 
         * Note: We are going to steal the two high bits from the local counter
         * (the sign bit, which is always zero since the localCounter is
         * non-negative, and the next bit, which is always zero since we
         * restrict the range of the localCounter using maxLocalCounter).
         * 
         * Note: When we reverse the bits, the two 2 high bits wind up as the
         * two low bits. They are simply overwritten by the two high bits of the
         * partitionId.
         */
        final int reversed = Integer.reverse(localCounter);

        if (DEBUG) {

            log.debug(Integer.toBinaryString(partitionId) + " : partitionId");

            log.debug(Integer.toBinaryString(localCounter) + " : localCounter");

            log.debug(Integer.toBinaryString(reversed) + " : reversed");

        }

        /*
         * OR the reversed localCounter into the high int32 word of the long id.
         * However, we need leave the sign bit of the long [id] alone so we left
         * shift by 31 rather than 32 bits. This leaves the low 2 bits of the
         * reversed localCounter in the low bit of the high int32 word and the
         * high bit of the low int32 word respectively. Those are the bits that
         * we are going to steal. The partitionId will step on both the high-bit
         * of the low int32 word and the low bit of the high-int32 word.
         * 
         * Note: You MUST cast the [int] values to [long] or left-shifting will
         * clear the high bits to zero.
         */
        long id = 0L;

        // note: the mask ensures that the high-bit remains clear.
        id |= (((long) reversed) << 31) & 0x7fffffff00000000L;

        if (DEBUG)
            log.debug(Long.toBinaryString(id) + " : id(a)");

        /*
         * OR the (long extended, left shifted by 2) partitionId into the long
         * id. This leaves the low two bits open for the bit flags. Since the
         * sign bit of the partition identifier is zero and since we left the
         * low two bits of the modified localCounter empty, this does not stomp
         * on any significant bits of the modified localCounter.
         */
        // extend to long and left-shift by 2 to make room for the bit flags.
        id |= (((long) partitionId) << 2);

        if (DEBUG)
            log.debug(Long.toBinaryString(id) + " : id(b)");

        /*
         * Set the bit flags for the Value type on the 2 low order bits.
         */
        switch (code) {

        case ITermIndexCodes.TERM_CODE_URI:

            id |= ITermIdCodes.TERMID_CODE_URI;

            break;

        case ITermIndexCodes.TERM_CODE_LIT:
        case ITermIndexCodes.TERM_CODE_DTL:
        case ITermIndexCodes.TERM_CODE_LCL:

            id |= ITermIdCodes.TERMID_CODE_LITERAL;

            break;

        case ITermIndexCodes.TERM_CODE_BND:

            id |= ITermIdCodes.TERMID_CODE_BNODE;

            break;

        case ITermIndexCodes.TERM_CODE_STMT:

            id |= ITermIdCodes.TERMID_CODE_STATEMENT;

            break;

        default:

            throw new AssertionError("Unknown term type: code=" + code);

        }

        if (DEBUG)
            log.debug(Long.toBinaryString(id) + " : id(final)");

        if (INFO) {

            final TermIdStat stat = decodeScaleOut(id);

            if (stat.partitionId != partitionId)
                throw new AssertionError("partitionId: in=" + partitionId
                        + ", out=" + stat.partitionId
                        + //
                        "\n" + Integer.toBinaryString(partitionId) + " : in"
                        + //
                        "\n" + Integer.toBinaryString(stat.partitionId)
                        + " : out"//
                );

            if (stat.localCounter != localCounter)
                throw new AssertionError("localCounter: in="
                        + localCounter//
                        + ", out=" + stat.localCounter
                        + //
                        "\n" + Integer.toBinaryString(localCounter) + " : in"
                        + //
                        "\n" + Integer.toBinaryString(stat.localCounter)
                        + " : out"//
                );

            switch (code) {
            case ITermIndexCodes.TERM_CODE_URI:
                if (!stat.isURI())
                    throw new AssertionError();
                break;
            case ITermIndexCodes.TERM_CODE_LIT:
            case ITermIndexCodes.TERM_CODE_DTL:
            case ITermIndexCodes.TERM_CODE_LCL:
                if (!stat.isLiteral())
                    throw new AssertionError();
                break;
            case ITermIndexCodes.TERM_CODE_BND:
                if (!stat.isBNode())
                    throw new AssertionError();
                break;
            case ITermIndexCodes.TERM_CODE_STMT:
                if (!stat.isSID())
                    throw new AssertionError();
                break;
            default:
                throw new AssertionError();
            }

        }

        return id;

    }

    /**
     * Data structure from an unpacked term identifier encoded by
     * {@link TermIdEncoder#encodeScaleOut(int, int, byte)}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TermIdStat {

        public final int partitionId;

        public final int localCounter;

        private final int flags;

        public TermIdStat(final int partitionId, final int localCounter,
                final int flags) {
            this.partitionId = partitionId;
            this.localCounter = localCounter;
            this.flags = flags;
        }

        public boolean isURI() {
            return flags == ITermIdCodes.TERMID_CODE_URI;
        }

        public boolean isLiteral() {
            return flags == ITermIdCodes.TERMID_CODE_LITERAL;
        }

        public boolean isBNode() {
            return flags == ITermIdCodes.TERMID_CODE_BNODE;
        }

        public boolean isSID() {
            return flags == ITermIdCodes.TERMID_CODE_STATEMENT;
        }

    }

    /**
     * Decodes a term identifier encoded by
     * {@link #encodeScaleOut(int, int, byte)}
     */
    static TermIdStat decodeScaleOut(final long id) {

        if (DEBUG)
            log.debug(Long.toBinaryString(id) + " : id (decode)");

        final int flags = (int) (id & ITermIdCodes.TERMID_CODE_MASK);

        if (DEBUG)
            log.debug(Integer.toBinaryString(flags) + " : flags");

        final int partitionId = (int) (id >>> 2) & 0x7fffffff;

        if (DEBUG)
            log.debug(Integer.toBinaryString(partitionId) + " : partitionId");

        final int reversed = (int) ((id & 0x7ffffffe00000000L) >>> 31);

        if (DEBUG)
            log.debug(Integer.toBinaryString(reversed) + " : reversed");

        final int localCounter = Integer.reverse(reversed);

        if (DEBUG)
            log.debug(Integer.toBinaryString(localCounter) + " : localCounter");

        return new TermIdStat(partitionId, localCounter, flags);

    }

}
