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
 * Created on May 3, 2010
 */

package com.bigdata.rdf.internal;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import com.bigdata.rdf.lexicon.ITermIdCodes;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

/**
 * Value Type Enumeration (IVTE) is a class with methods for interpreting and 
 * setting the bit flags used to identify the type of an RDF Value (URI, 
 * Literal, Blank Node, SID, etc).
 * 
 * @todo This replaces {@link ITermIdCodes}.
 * 
 * @todo update {@link TermIdEncoder}. This encodes term identifiers for
 *       scale-out but moving some bits around. It will be simpler now that the
 *       term identifier is all bits in the long integer with an additional byte
 *       prefix to differentiate URI vs Literal vs BNode vs SID and to indicate
 *       the inline value type (termId vs everything else).
 */
public enum VTE implements ITermIdCodes {

    /** A URI. */
    URI((byte) 0x00),
    
    /** A blank node. */
    BNODE((byte) 0x01),
    
    /** A literal. */
    LITERAL((byte) 0x02),

    /** A statement identifier. */
    STATEMENT((byte) 0x03);

    private VTE(final byte flags) {

        this.v = flags;
        
    }

    /**
     * Note: This field is package private so it is visible to
     * {@link AbstractInternalValue}.
     */
    final byte v;

    /**
     * Return the {@link VTE} identified by the LOW TWO (2)
     * bits in the caller's value.
     * 
     * @param b
     *            The bit flags.
     * @return The corresponding {@link VTE}.
     */
    static public VTE valueOf(final byte b) {
        /*
         * Note: Java does not permit the construct URI.v in the cases of the
         * switch (it is not interpreted as a constant). Therefore the switch
         * cases are hardwired to the values specified for each of the 4 RDF
         * Value types above.
         */
        switch (b & 0x03) { // mask off everything but the low 2 bits.
        case 0x00:
            return URI;
        case 0x01:
            return BNODE;
        case 0x02:
            return LITERAL;
        case 0x03:
            return STATEMENT;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Return the {@link VTE} identified by the LOW TWO (2)
     * bits in the caller's value.
     * 
     * @param b
     *            The bit flags.
     * @return The corresponding {@link VTE}.
     */
    static public VTE valueOf(final long l) {
        if (isURI(l))
            return URI;
        if (isBNode(l))
            return BNODE;
        if (isLiteral(l))
            return LITERAL;
        if (isStatement(l))
            return STATEMENT;
        
        throw new AssertionError();
    }

    /**
     * Return the one character code for this RDF Value type (U, L, B, or S).
     * This is used in various internal toString() implementations.
     */
    public final char getCharCode() {
        if (v == URI.v)
            return 'U';
        else if (v == LITERAL.v)
            return 'L';
        else if (v == BNODE.v)
            return 'B';
        else if (v == STATEMENT.v)
            return 'S';
        throw new AssertionError();
    }
    
    /**
     * Return true iff the term identifier is marked as a RDF {@link Literal}.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a {@link Literal}.
     * <p>
     * Note: Some entailments require the ability to filter based on whether or
     * not a term is a literal. For example, literals may not be entailed into
     * the subject position. This method makes it possible to determine whether
     * or not a term is a literal without materializing the term, thereby
     * allowing the entailments to be computed purely within the term identifier
     * space.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is marked as an RDF
     *         {@link Literal}.
     */
    static final public boolean isLiteral(final long termId) {

        return (termId & TERMID_CODE_MASK) == TERMID_CODE_LITERAL;

    }

    /**
     * Return true iff the term identifier is marked as a RDF {@link BNode}.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a {@link BNode}.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is marked as an RDF
     *         {@link BNode}.
     */
    static final public boolean isBNode(final long termId) {

        return (termId & TERMID_CODE_MASK) == TERMID_CODE_BNODE;

    }

    /**
     * Return true iff the term identifier is marked as a RDF {@link URI}.
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is a {@link URI}.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier is marked as an RDF
     *         {@link URI}.
     */
    static final public boolean isURI(final long termId) {

        /*
         * Note: The additional != NULL test is necessary for a URI term
         * identifier so that it will not report 'true' for 0L since the two low
         * order bits used to mark a URI are both zero.
         */
        
        return (termId & TERMID_CODE_MASK) == TERMID_CODE_URI && termId != TermId.NULL;

    }

    /**
     * Return true iff the term identifier identifies a statement (this feature
     * is enabled with {@link Options#STATEMENT_IDENTIFIERS}).
     * <p>
     * Note: This simply examines the low bits of the term identifier, which
     * marks whether or not the term identifier is actually a statement
     * identifier.
     * 
     * @param termId
     *            The term identifier.
     * 
     * @return <code>true</code> iff the term identifier identifies a
     *         statement.
     */
    static final public boolean isStatement(final long termId) {

        return (termId & TERMID_CODE_MASK) == TERMID_CODE_STATEMENT;

    }

}
