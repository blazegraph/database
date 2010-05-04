package com.bigdata.rdf.internal;

import java.security.MessageDigest;
import java.util.UUID;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Configuration determines which RDF Values are inlined into the statement
 * indices rather than being assigned term identifiers by the lexicon.
 */
public interface BigdataInlineValueConfig {

    /**
     * <code>true</code> iff <code>xsd:boolean</code> should be inlined.
     */
    public boolean isBooleanInline();

    /**
     * <code>true</code> iff the fixed size numerics (<code>xsd:int</code>,
     * <code>xsd:short</code>, <code>xsd:float</code>, etc) should be inlined.
     */
    public boolean isSmallNumericInline();

    /**
     * <code>true</code> iff xsd:integer should be inlined.
     */
    public boolean isXSDIntegerInline();

    /**
     * <code>true</code> iff <code>xsd:decimal</code> should be inlined.
     * 
     * @todo This option is not yet supported. Combine with XSDInteger for
     *       isBigNumericInline()?
     */
    public boolean isXSDDecimalInline();

    /**
     * <code>true</code> iff blank node identifiers should be inlined. This
     * is only possible when the blank node identifiers are internally
     * generated {@link UUID}s since otherwise they can be arbitrary Unicode
     * strings which, like text-based Literals, can not be inlined.
     * <p>
     * This option is NOT compatible with
     * {@link AbstractTripleStore.Options#STORE_BLANK_NODES}.
     * 
     * @todo Separate option to inlined SIDs?
     */
    public boolean isBlankNodeInline();

    /**
     * <code>true</code> if UUID values (other than blank nodes) should be
     * inlined.
     */
    public boolean isUUIDInline();

    /**
     * @todo Option to enable storing of long literals (over a configured
     *       threshold) as blob references. The TERM2ID index would have a
     *       hash function (MD5, SHA-1, SHA-2, etc) of the value and assign
     *       a termId. The ID2TERM index would map the termId to a blob
     *       reference. The blob data would be stored in the journal and
     *       migrate into index segments during overflow processing for
     *       scale-out.
     */
    public boolean isLongLiteralAsBlob();

    /**
     * Return the {@link MessageDigest} used to compute a hash code for a long
     * literal. The message digest should compute a hash function with a very
     * small probability of collisions. In general, <code>SHA-256</code> (32
     * bytes), <code>SHA-384</code> (48 bytes) and <code>SHA-512</code> (64
     * byte) should be reasonable choices.
     * <p>
     * Appropriate hash algorithms are defined in the <a
     * href="http://csrc.nist.gov/publications/fips/index.html">FIPS PUB
     * 180-2</a> (which has been replaced by <a href=
     * "http://csrc.nist.gov/publications/fips/fips180-3/fips180-3_final.pdf"
     * >FIPS PUB 180-3</a>. Also see Recommendation for Applications Using
     * Approved Hash Algorithms in <a href=
     * "http://csrc.nist.gov/publications/nistpubs/800-107/NIST-SP-800-107.pdf"
     * >SP 800-107</a>, which provides information about the collision
     * resistance of these hash algorithms.
     * 
     * @return A {@link MessageDigest} object which can be used to compute the
     *         hash code for a long literal.
     */
    public MessageDigest getLongLiteralMessageDigest();
    
}
