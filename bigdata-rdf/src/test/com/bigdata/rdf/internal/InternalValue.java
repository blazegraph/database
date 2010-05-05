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

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore.Options;

/**
 * Interface for the internal representation of an RDF Value (the representation
 * which is encoded within the statement indices).
 * 
 * @todo Consider whether we need the ability to compute the successor of a
 *       value in the value space here. There are implementations of successor()
 *       for most data types in {@link SuccessorUtil}, including fixed length
 *       unsigned byte[]s, and also {@link BytesUtil#successor(byte[])}, which
 *       handles variable length unsigned byte[]s.
 */
public interface InternalValue<V extends BigdataValue, T> extends Serializable {

    /**
     * The value of the flags representing the {@link InternalValueTypeEnum} and
     * the {@link InternalDataTypeEnum}. The upper TWO (2) bits code the
     * {@link InternalValueTypeEnum} while the lower SIX (6) bits code the
     * {@link InternalDataTypeEnum}.
     */
    byte flags();

    /*
     * RDF Value type methods.
     */

    /**
     * Return the {@link InternalValueTypeEnum} for the {@link InternalValue}
     */
    InternalValueTypeEnum getInternalValueTypeEnum();

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
     * Return the {@link InternalDataTypeEnum} for the {@link InternalValue} .
     * This will be {@link InternalDataTypeEnum#TermId} iff the internal "value"
     * is a term identifier. Otherwise it will be the type safe enum
     * corresponding to the specific data type which can be decoded from this
     * {@link InternalValue} using {@link #getInlineValue()}.
     */
    InternalDataTypeEnum getInternalDataTypeEnum();

    /**
     * <code>true</code> iff the RDF value is represented by a term identifier.
     * When an RDF Value is represented as a term identifier, it must be
     * resolved against the <code>ID2TERM</code> index.
     * 
     * @see #isInline()
     */
    boolean isTermId();

    /**
     * <code>true</code> iff the RDF value is a term identifier whose value is
     * <code>0L</code>.
     * 
     * @see #isTermId()
     */
    boolean isNull();

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
    boolean isShortNumeric();

    /**
     * <code>true</code> for xsd:integer and xsd:decimal.
     */
    boolean isBigNumeric();

    /**
     * Inflate an inline RDF value to a {@link BigdataValue}. This method DOES
     * NOT guarantee a singleton pattern for the inflated value and the value
     * factory. However, implementations are encouraged to cache the inflated
     * {@link BigdataValue} on a transient field.
     * 
     * @param f
     *            The value factory.
     * @return The corresponding {@link BigdataValue}.
     * @throws UnsupportedOperationException
     *             unless the RDF value is inline.
     * 
     *             FIXME Reconcile with BigdataValueImpl and BigdataValue. The
     *             role of the valueFactory reference on BigdataValueImpl was to
     *             detect when an instance was created by another value factory.
     *             The choice of whether or not to inline the value is
     *             determined by the lexicon configuration, and that choice is
     *             probably captured by a BigdataValueFactory configuration
     *             object. Therefore we do need to convert to a different
     *             instance when the {@link InternalValue} will be used in a
     *             different lexicon configuration context.
     *             <P>
     *             It would be nice to support shared lexicons for a collection
     *             of triple / quad stores. The lexicon would be in the
     *             container namespace for that federation of KBs. The
     *             individual triple/quad stores would be in the per-KB instance
     *             namespace. The collection could have a mixture of triple
     *             and/or quad stores since the lexicon does not interact with
     *             whether we are using triples or quads (except for SIDs).
     */
    V asValue(BigdataValueFactory f) throws UnsupportedOperationException;

}
