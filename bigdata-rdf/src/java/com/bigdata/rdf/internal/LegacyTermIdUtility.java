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
 * Created July 10, 2010
 */

package com.bigdata.rdf.internal;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.ITermIdCodes;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

class LegacyTermIdUtility implements ITermIdCodes {
    
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

    /**
     * Return the {@link VTE} identified by the LOW TWO (2)
     * bits in the caller's value.
     * 
     * @param b
     *            The bit flags.
     * @return The corresponding {@link VTE}.
     */
    static final public VTE VTE(final long l) {
        if (isURI(l))
            return VTE.URI;
        if (isBNode(l))
            return VTE.BNODE;
        if (isLiteral(l))
            return VTE.LITERAL;
        if (isStatement(l))
            return VTE.STATEMENT;
        
        throw new AssertionError(String.valueOf(l));
    }

    /**
     * Construct a new style {@link TermId} from an old style long tid.  The 
     * term type is encoded into the long in the old style long tid and will
     * be converted to a {@link VTE} by this method.
     * 
     * @param legacyTid
     *          The old style long term identifier.  See {@link ITermIdCodes}.
     */
    static final public TermId termId(final long legacyTid) {
    
        if (legacyTid == TermId.NULL)
            return null;
        
        return new TermId(VTE(legacyTid), legacyTid);
        
    }
    
    /**
     * Encode an IV using the old style data scheme, which is to just write the
     * long term identifier without the flags. 
     * 
     * @param keyBuilder
     *          The object used to encode the {@link IV}.
     * @param iv
     *          The {@link IV} to encode.
     */
    static final public IKeyBuilder encode(IKeyBuilder keyBuilder, IV iv) {
     
        if (iv.isInline()) {
            throw new IllegalArgumentException();
        }
        
        keyBuilder.append(iv.getTermId());
        
        return keyBuilder;
        
    }
    
    /**
     * Decode an IV using the old style data scheme, which is to just read the
     * long term identifier without any flags.
     * 
     * @param key
     *            The byte[].
     * @return The {@link IV} or <code>null</code> if the 
     *            <code>tid == {@link TermId#NULL}</code>.
     */
    static final public TermId decode(byte[] key) {

        final long tid = KeyBuilder.decodeLong(key, 0);
        
        return termId(tid);
        
    }
    
    /**
     * Decode a set of {@link IV}s from a byte[].
     * 
     * @param key
     *            The byte[].
     * @return The set of {@link IV}s.
     * 
     *         FIXME handle all of the inline value types.
     * 
     *         FIXME Construct the InternalValue objects using factory if we
     *         will have to scope how the RDF Value is represented to the
     *         lexicon relation with which it is associated?
     */
    public static IV[] decode(final byte[] key, final IV[] ivs) {

        final int arity = ivs.length;
        
        int offset = 0;
        
        for (int i = 0; i < arity; i++) {
        
            // decode the term identifier.
            final long termId = KeyBuilder.decodeLong(key, offset);
            offset += Bytes.SIZEOF_LONG;

            /*
             * FIXME this is here for now until 
             * {@link AbstractInternalValue#isNull(byte)} works.
             */
            if (termId == TermId.NULL) {
                
                ivs[i] = null;
                
            } else {

                ivs[i] = termId(termId);
            
            }
                
        }

        return ivs;
        
    }
    
}
