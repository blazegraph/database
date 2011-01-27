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

package com.bigdata.rdf.internal;

import org.openrdf.model.Value;

import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Configuration determines which RDF Values are inlined into the statement
 * indices rather than being assigned term identifiers by the lexicon.
 */
public interface ILexiconConfiguration<V extends BigdataValue> {

    /**
     * Create an inline {@link IV} for the supplied RDF value if inlining is
     * supported for the supplied RDF value. 
     * <p>
     * If the supplied RDF value is a {@link BigdataValue} then the {@link IV}
     * will be set as a side-effect.
     * 
     * @param value
     *          the RDF value
     * @return
     *          the inline {@link IV}
     */
    IV createInlineIV(final Value value);
    
    /**
     * Create an RDF value from an {@link ExtensionIV}. Looks through an
     * internal catalog of {@link IExtension}s to find one that knows how to
     * handle the extension datatype from the supplied {@link ExtensionIV}.
     * 
     * @param iv
     *          the extension IV
     * @param vf
     *          the bigdata value factory
     * @return
     *          the RDF value
     */
    V asValue(final ExtensionIV iv, final BigdataValueFactory vf);
    
    /**
     * Initialize the extensions, which need to resolve their datatype URIs
     * into term ids.
     */
    void initExtensions(final LexiconRelation lex);
    
    /**
     * <code>true</code> iff the <code>vte</code> and <code>dte</code> 
     * should be inlined.
     * 
     * @param vte
     *          the term type
     * @param dte
     *          the data type
    public boolean isInline(VTE vte, DTE dte);
     */
    
//    /**
//     * <code>true</code> iff <code>xsd:boolean</code> should be inlined.
//     */
//    public boolean isBooleanInline();
//
//    /**
//     * <code>true</code> iff the fixed size numerics (<code>xsd:int</code>,
//     * <code>xsd:short</code>, <code>xsd:float</code>, etc) should be inlined.
//     */
//    public boolean isSmallNumericInline();
//
//    /**
//     * <code>true</code> iff xsd:integer should be inlined.
//     * <p>
//     * Note: The maximum length for the encoding is ~32kb per key. With a B+Tree
//     * branching factor of 256 that is ~ 8MB per leaf before compression. While
//     * that is definitely large, it is not so outrageous that we need to forbid
//     * it.
//     */
//    public boolean isXSDIntegerInline();
//
//    /**
//     * <code>true</code> iff <code>xsd:decimal</code> should be inlined.
//     */
//    public boolean isXSDDecimalInline();
//
//    /**
//     * <code>true</code> iff blank node identifiers should be inlined. This
//     * is only possible when the blank node identifiers are internally
//     * generated {@link UUID}s since otherwise they can be arbitrary Unicode
//     * strings which, like text-based Literals, can not be inlined.
//     * <p>
//     * This option is NOT compatible with
//     * {@link AbstractTripleStore.Options#STORE_BLANK_NODES}.
//     */
//    public boolean isBlankNodeInline();
//
//    /**
//     * <code>true</code> if UUID values (other than blank nodes) should be
//     * inlined.
//     */
//    public boolean isUUIDInline();
//
//    /**
//     * Option to enable storing of long literals (over a configured
//     *       threshold) as blob references. The TERM2ID index would have a
//     *       hash function (MD5, SHA-1, SHA-2, etc) of the value and assign
//     *       a termId. The ID2TERM index would map the termId to a blob
//     *       reference. The blob data would be stored in the journal and
//     *       migrate into index segments during overflow processing for
//     *       scale-out.
//     */
//    public boolean isLongLiteralAsBlob();
//
//    /**
//     * Return the {@link MessageDigest} used to compute a hash code for a long
//     * literal. The message digest should compute a hash function with a very
//     * small probability of collisions. In general, <code>SHA-256</code> (32
//     * bytes), <code>SHA-384</code> (48 bytes) and <code>SHA-512</code> (64
//     * byte) should be reasonable choices.
//     * <p>
//     * Appropriate hash algorithms are defined in the <a
//     * href="http://csrc.nist.gov/publications/fips/index.html">FIPS PUB
//     * 180-2</a> (which has been replaced by <a href=
//     * "http://csrc.nist.gov/publications/fips/fips180-3/fips180-3_final.pdf"
//     * >FIPS PUB 180-3</a>. Also see Recommendation for Applications Using
//     * Approved Hash Algorithms in <a href=
//     * "http://csrc.nist.gov/publications/nistpubs/800-107/NIST-SP-800-107.pdf"
//     * >SP 800-107</a>, which provides information about the collision
//     * resistance of these hash algorithms.
//     * 
//     * @return A {@link MessageDigest} object which can be used to compute the
//     *         hash code for a long literal.
//     */
//    public MessageDigest getLongLiteralMessageDigest();
//    
}
