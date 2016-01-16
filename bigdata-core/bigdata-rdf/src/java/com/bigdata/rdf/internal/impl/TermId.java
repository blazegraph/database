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
package com.bigdata.rdf.internal.impl;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.LongPacker;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.literal.NumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.util.Bytes;

/**
 * Implementation for any kind of RDF {@link Value} when the values is not being
 * inlined. Instances of this class can represent {@link URI}s, {@link BNode}s
 * (if they are not being inlined), or {@link Literal}s (including datatype
 * literals if they are not being inlined). Larger RDF {@link Value}s should be
 * represented with {@link BlobIV} instead of this class.
 */
public class TermId<V extends BigdataValue>
        extends AbstractNonInlineIV<V, Void> {

    /**
     * 
     */
    private static final long serialVersionUID = 4309045651680610931L;
    
    /**
     * Value used for a "NULL" term identifier.
     */
    public static final transient long NULL = 0L;

    /** The term identifier. */
    private final long termId;

    final public boolean isNullIV() {

        return termId == NULL;

    }

    /**
     * Singleton for a "null" {@link IV}.
     */
    final public static transient TermId<?> NullIV = TermId.mockIV(VTE.URI);

    /**
     * Create a mock {@link IV} having the indicated {@link VTE} which will
     * report <code>true</code> for {@link #isNullIV()}. This is used by some
     * code patterns where we need to associate a {@link BigdataValue} not in
     * the database with an {@link IV} on a temporary basis.
     * 
     * @param vte
     *            The {@link VTE}.
     * 
     * @return The mock {@link IV}.
     */
    @SuppressWarnings("rawtypes")
    static public TermId<?> mockIV(final VTE vte) {

        /*
         * Note: This MUST be consistent with TermsIndexHelper#makeKey() !!!
         */

        return new TermId(vte, NULL);

    }

    @Override
    public IV<V, Void> clone(final boolean clearCache) {

        final TermId<V> tmp = new TermId<V>(flags, termId);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    /**
     * Constructor for a term identifier when you are decoding and already have
     * the flags.
     * 
     * @param flags
     *            The flags
     * @param termId
     */
    public TermId(final byte flags, final long termId) {

        super(flags);

        this.termId = termId;
        
    }

    /**
     * Constructor for a term identifier.
     * @param vte
     * @param termId
     */
    public TermId(final VTE vte, final long termId) {

        /*
         * Note: XSDBoolean happens to be assigned the code value of 0, which is
         * the value we want when the data type enumeration will be ignored.
         */
        super(vte, false/* extension */, DTE.XSDBoolean);

        this.termId = termId;
        
    }
    
    /**
     * Human readable representation includes the term identifier, whether this
     * is a URI, Literal, Blank node, or Statement identifier and the datatype
     * URI if one is assigned. This representation is based solely on the flag
     * bits and the term identifier.
     * <p>
     * The cached {@link BigdataValue}, if any, is also rendered. This is done
     * using {@link BigdataValue#stringValue()} in order to avoid possible
     * infinite recursion through {@link BigdataValue#toString()} if the latter
     * were to display the {@link IV}.
     */
    @Override
    public String toString() {

        return "TermId(" + termId + String.valueOf(getVTE().getCharCode())
                + ")"
                + (hasValue() ? "[" + getValue().stringValue() + "]" : "");

    }

    /**
     * Decodes the output of {@link #toString()}, returning a new {@link TermId}
     * .
     * 
     * @param s
     *            The string representation.
     * 
     * @return The {@link TermIV}.
     */
    static public TermId<?> fromString(final String s) {

        final int pos = s.indexOf("[");
        
        final int end = (pos > 0 ? pos : s.length()) - 2;

        final long tid = Long.valueOf(s.substring(7, end));
        
        final char type = s.charAt(end);

        @SuppressWarnings("rawtypes")
        final TermId<?> tmp = new TermId(VTE.valueOf(type), tid);

        return tmp;

    }
    
    /**
     * Return the termId.
     */
    final public long getTermId() {

        return termId;
        
    }

    /**
     * {@inheritDoc
     * 
     * Note: only the termId matters for equality, unless either is #NULL, in
     * which case this will compare the cached {@link BigdataValue}s if they
     * exist.  Null IVs without cached values are never equal.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof TermId<?>) {
            final TermId<?> t = (TermId<?>) o;
            if (this.termId == NULL || t.termId == NULL) {
                if (this.hasValue() && t.hasValue()) {
                    return this.getValue().equals(t.getValue());
                } else {
                    return false;
                }
            } else {
                return termId == t.termId;
            }
            //            return termId == ((TermId<?>) o).termId;
        }
        return false;
    }

    /**
     * Return the hash code of the long term identifier.
     * 
     * @see Long#hashCode()
     */
    @Override
    public int hashCode() {
        
        return (int) (termId ^ (termId >>> 32));
        
    }

    @Override
    public int byteLength() {

        if (IVUtility.PACK_TIDS) {

            return 1 + LongPacker.getByteLength(termId);

        } else {
            
            return 1 + Bytes.SIZEOF_LONG;
            
        }
        
    }

    @Override
    public int _compareTo(final IV o) {
        
        final long termId2 = ((TermId<?>) o).termId;
        
        return termId < termId2 ? -1 : termId > termId2 ? 1 : 0; 
        
    }
    
    @Override
    public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

        // First emit the flags byte.
        keyBuilder.appendSigned(flags());

        if (IVUtility.PACK_TIDS) {

            // variable length encoding
            ((KeyBuilder) keyBuilder).pack(termId);
            
        } else {
            
            // fixed length encoding.
            keyBuilder.append(termId);
            
        }
        
        return keyBuilder;
        
    }

    /**
     * Overrides {@link BNode#getID()}.
     * <p>
     * Creates a unique blank node ID based on the {@link TermId}'s internal
     * data.
     */
    @Override
    public String getID() {

        return Long.toString(termId);

    }
    
    @Override
    public boolean isNumeric() {
        
        V value = getValue();
        if (value==null)
            throw new NotMaterializedException();
        
        if (!(value instanceof BigdataLiteral))
            return false;

        return NumericIV.numericalDatatypes.contains(((BigdataLiteral)value).getDatatype());
        
    }

//    /**
//     * Override default serialization to send the cached {@link BigdataValue}.
//     * 
//     * @see https://sourceforge.net/apps/trac/bigdata/ticket/337
//     */
//	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
//
//		out.defaultWriteObject();
//		
//		out.writeObject(getValueCache());
//
//	}
//
//	/**
//	 * Override default serialization to recover the cached {@link BigdataValue}
//	 * .
//	 */
//	@SuppressWarnings("unchecked")
//	private void readObject(java.io.ObjectInputStream in) throws IOException,
//			ClassNotFoundException {
//
//		in.defaultReadObject();
//
//		final V v = (V) in.readObject();
//
//		if (v != null) {
//			// set the value cache.
//			setValue(v);
//		}
//		
//	}

}
