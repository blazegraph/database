/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Interface for the internal representation of an RDF {@link Value} (the
 * representation which is encoded within the statement indices).
 * 
 * @param <V>
 *            The generic type for the RDF {@link Value} implementation.
 * @param <T>
 *            The generic type for the inline value.
 */
public interface IV<V extends BigdataValue, T> extends Serializable, 
        Comparable<IV>, IVCache<V,T>, Value {

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

    /** 
     * Return <code>true</code> iff this is an RDF BlankNode. 
     */
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
    
    /**
     * Return <code>true</code> iff this is a URI or a bnode.
     */
    boolean isResource();

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
     * IFF {@link #getDTE()} returns {@link DTE#Extension} then this method will
     * report the {@link DTEExtension} value that specifies the intrinsic
     * datatype for this IV.
     * 
     * @see BLZG-1507 (Implement support for DTE extension types for URIs)
     * 
     * @see BLZG-1595 ( DTEExtension for compressed timestamp)
     */
    DTEExtension getDTEX();
    
	/**
	 * <code>true</code> iff the {@link IV} represents a <em>null</em>
	 * {@link IV} reference. <code>null</code> {@link IV}s are somewhat special.
	 * They get used as wild cards for the keys in the justifications index and
	 * perhaps (?) in a few other locations.
	 */
    boolean isNullIV();

    /**
     * <code>true</code> iff the RDF value is directly represented inline. When
     * an RDF Value is "inline" its value can be directly decoded from its
     * representation in the keys of the statement indices. 
     */
    boolean isInline();

	/**
	 * <code>true</code> iff the <code>flags</code> byte is followed by an
	 * {@link IV} which defines how the subsequent value (represented according
	 * to the {@link DTE}) will be interpreted. This is used to support
	 * projections of value spaces for data type literals onto the intrinsic
	 * types. It is also used to support indirect resolution of the namespace
	 * associated with a URI.
	 */
    boolean isExtension();

    /**
     * Return <code>true</code> iff this {@link IV} is a {@link Vocabulary} 
     * item.
     */
    boolean isVocabulary();
    
    /**
     * Return the Java {@link Object} corresponding to the inline value.
     * 
     * @return The {@link Object}.
     * @throws UnsupportedOperationException
     *             unless the RDF value is inline.
     */
    T getInlineValue() throws UnsupportedOperationException;
    
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
//
//    /**
//     * Return the blank node ID for this {@link IV}.
//     * 
//     * @throws UnsupportedOperationException
//     *             if this {@link IV} does not represent a blank node.
//     * @return
//     */
//    String bnodeId();
    
    /**
     * Each concrete {@link IV} implementation will implement one of the
     * corresponding openrdf {@link Value} interfaces depending on the type of
     * value the {@link IV} represents ({@link URI}, {@link BNode}, or
     * {@link Literal}). This method signifies whether or not the IV can deliver
     * the information needed by those interfaces with or without
     * materialization. For example, inline numerics can implement the entire
     * {@link Literal} interface without needing to be materialized into a
     * {@link BigdataLiteral}. {@link TermId}s cannot answer any of the requests
     * in the openrdf interfaces without materialization (all the relevant
     * information is in the lexicon indices). Even some inlines need
     * materialization. For example, ...
     */
    boolean needsMaterialization();

}
