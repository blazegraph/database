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

import java.io.Serializable;

import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

/**
 * Interface for the internal representation of an RDF Value (the representation
 * which is encoded within the statement indices).
 * 
 * @param <V>
 *            The generic type for the RDF {@link Value} implementation.
 * @param <T>
 *            The generic type for the inline value.
 */
public interface IV<V extends BigdataValue, T> extends Serializable, 
        Comparable<IV> {

    /**
     * The value of the flags representing the {@link VTE} and the {@link DTE}.
     * The upper TWO (2) bits code the {@link VTE} while the lower SIX (6) bits
     * code the {@link DTE}.
     */
    byte flags();

    /**
     * The byte length of the encoded {@link IV}.
     */
    int byteLength();

    /**
     * Encode the {@link IV} as an unsigned byte[].
     * 
     * @param keyBuilder
     *            The object used to encode the {@link IV}.
     * @return the key builder
     */
    IKeyBuilder encode(IKeyBuilder keyBuilder);
    
    /*
     * RDF Value type methods.
     */

    /**
     * Return the {@link VTE} for the {@link IV}
     */
    VTE getVTE();

    /**
     * Return <code>true</code> iff this is an RDF Literal. Note that some kinds
     * of RDF Literals MAY be represented inline.
     */
    boolean isLiteral();

    /** Return <code>true</code> iff this is an RDF BlankNode. */
    boolean isBNode();

    /**
     * Return <code>true</code> iff this is an RDF {@link URI}.
     */
    boolean isURI();

    /**
     * Return <code>true</code> iff this is a statement identifier (this feature
     * is enabled with {@link Options#STATEMENT_IDENTIFIERS}).
     */
    boolean isStatement();

    /*
     * Data type methods.
     */

    /**
     * Return the {@link DTE} for the {@link IV} .
     * This will be {@link DTE#TermId} iff the internal "value"
     * is a term identifier. Otherwise it will be the type safe enum
     * corresponding to the specific data type which can be decoded from this
     * {@link IV} using {@link #getInlineValue()}.
     */
    DTE getDTE();

    /**
     * <code>true</code> iff the RDF value is represented by a term identifier.
     * When an RDF Value is represented as a term identifier, it must be
     * resolved against the <code>ID2TERM</code> index.
     * 
     * @see #isInline()
     */
    boolean isTermId();

    /**
     * Return the term identifier.
     * 
     * @return The term identifier.
     * @throws UnsupportedOperationException
     *             unless the RDF value is represented by a term identifier.
     */
    long getTermId() throws UnsupportedOperationException;

    /**
     * <code>true</code> iff the RDF value is directly represented inline. When
     * an RDF Value is "inline" its value can be directly decoded from its
     * representation in the keys of the statement indices. This is in contrast
     * to having to resolve a term identifier to its value using the
     * <code>ID2TERM</code> index.
     * 
     * @see #isTermId()
     */
    boolean isInline();

    /**
     * Return the Java {@link Object} corresponding to the inline value.
     * 
     * @return The {@link Object}.
     * @throws UnsupportedOperationException
     *             unless the RDF value is inline.
     */
    T getInlineValue() throws UnsupportedOperationException;

//    /**
//     * 
//     * @return
//     * @throws NoSuccessorException
//     */
//    T successor() throws NoSuccessorException;
    
    /**
     * <code>true</code> for any of the numeric data types (xsd:byte,
     * xsd:unsignedByte, xsd:short, xsd:unsignedShort, xsd:int, xsd:unsignedInt,
     * xsd:long, xsd:unsignedLong, xsd:float, xsd:double, xsd:integer, and
     * xsd:decimal).
     */
    boolean isNumeric();

    /**
     * <code>true</code> for an signed numeric datatype ( xsd:byte,
     * xsd:short, xsd:int, xsd:long, xsd:float, xsd:double, xsd:integer, and
     * xsd:decimal).
     */
    boolean isSignedNumeric();

    /**
     * <code>true</code> for an unsigned numeric datatype ( xsd:unsignedByte,
     * xsd:unsignedShort, xsd:unsignedInt, xsd:unsignedLong).
     */
    boolean isUnsignedNumeric();

    /**
     * This is <code>!isBigNumeric()</code> and is <code>true</code> for any of
     * the fixed length numeric data types (xsd:byte, xsd:unsignedByte,
     * xsd:short, xsd:unsignedShort, xsd:int, xsd:unsignedInt, xsd:long,
     * xsd:unsignedLong, xsd:float, xsd:double).
     */
    boolean isFixedNumeric();

    /**
     * <code>true</code> for xsd:integer and xsd:decimal.
     */
    boolean isBigNumeric();
    
    /**
     * <code>true</code> for xsd:float, xsd:double, and xsd:decimal
     */
    boolean isFloatingPointNumeric();

	/**
	 * If the value is not already cached, then inflate an inline RDF value to a
	 * {@link BigdataValue} and cache it on a private field.
	 * <p>
	 * Note: Query plans are responsible for ensuring that {@link IV}s have been
	 * materialized before operators are evaluated which invoke this method.
	 * This pattern ensures that efficient batch operators are used to
	 * materialize {@link Value}s, and thereby avoids heavy RMI overhead in
	 * scale-out, and provides operators which use {@link #getValue()} with a
	 * simple method signature which does not require access to the lexicon.
	 * Query plans are also responsible for dropping variables once they are no
	 * longer needed or, in the case of large values and BLOBs, dropping the
	 * cached {@link BigdataValue} when possible in order to avoid excess
	 * network and heap overhead.
	 * 
	 * @param lex
	 *            The lexicon relation (this is required in order to access the
	 *            {@link BigdataValueFactory} for the namespace associated with
	 *            lexicon when we materialize an inline {@link IV}).
	 * 
	 * @return The corresponding {@link BigdataValue}.
	 * 
	 * @throws UnsupportedOperationException
	 *             if the {@link IV} does not represent something which can be
	 *             materialized. For example, a dummy value or a "null".
	 */
    V asValue(final LexiconRelation lex) 
        throws UnsupportedOperationException;

	/**
	 * Return a pre-materialized RDF {@link BigdataValue} which has been cached
	 * on this {@link IV} by a previous invocation of
	 * {@link #asValue(LexiconRelation)}.
	 * 
	 * @return The {@link BigdataValue}.
	 * 
	 * @throws NotMaterializedException
	 *             if the value is not cached.
	 */
	V getValue() throws NotMaterializedException;

	/**
	 * Drop the cached {@link BigdataValue}. This is a NOP if the cache is
	 * empty.
	 */
	void dropValue();
	
}
