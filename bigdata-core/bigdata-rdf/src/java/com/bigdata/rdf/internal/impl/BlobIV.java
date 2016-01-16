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
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.INonInlineExtensionCodes;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Implementation for any kind of RDF {@link Value} when the value is not being
 * inlined, but primarily used with large RDF {@link Value}s. Instances of this
 * class can represent {@link URI}s, {@link BNode}s (if they are not being
 * inlined), or {@link Literal}s (including datatype literals if they are not
 * being inlined). The representation of the {@link BlobIV} is simply the key in
 * the TERMS index for the tuple for a given RDF {@link Value}. While flexible,
 * {@link Value}s models as {@link BlobIV}s require indirection through the
 * TERMS index in order to materialize the {@link Value}. We are often able to
 * inline {@link Value}s compactly and efficiently into the statement indices,
 * in which case those values simply do not appear in the TERMS index. The
 * {@link BlobIV} remains useful when the RDF {@link Value} is large and not
 * otherwise amenable to inlining.
 * <p>
 * Note: The {@link BlobIV} uses a hash code based on the hash code of the RDF
 * {@link Value}. This gives us a fixed length key for the TERMS index, even
 * when the RDF {@link Value} is very large. This makes the {@link BlobIV}
 * suitable for use with very large RDF {@link Value}s. However, the hash code
 * results in widely dispersed updates which cause a lot of IO scatter. This
 * makes the TERMS index slower on write than the TERM2ID and ID2TERM indices.
 * Therefore shorter RDF {@link Value}s should be inserted by preference into
 * the TERM2ID / ID2TERM indices while larger values should be inserted as
 * {@link BlobIV}s into the TERMS index.
 */
public class BlobIV<V extends BigdataValue> extends
        AbstractNonInlineExtensionIV<V, Void> {

    /**
     * 
     */
//    private static final long serialVersionUID = 4309045651680610931L;
    private static final long serialVersionUID = 2;
    
    /**
     * Return the <code>flags</code> byte for a {@link BlobIV}. 
     */
	public static final byte toFlags(final VTE vte) {
		/*
		 * Note: XSDBoolean happens to be assigned the code value of 0, which is
		 * the value we want when the data type enumeration will be ignored.
		 */
		return AbstractIV.toFlags(vte, false/* inline */,
				true/* extension */, DTE.XSDBoolean);
	}

////    /**
////     * {@inheritDoc}
////     * <p>
////     * This checks the hashCode and the counter, both of which must decode to
////     * ZERO (0). The {@link VTE} may be any of the possible {@link VTE}s. "null"
////     * versions of different kinds of {@link VTE}s are created by
////     * {@link #mockIV(VTE)}.
////     * 
////     * @see LangBOp
////     * @see DatatypeBOp
////     * @see FuncBOp
////     * @see BigdataEvaluationStrategyImpl3
////     */
////    @Override
//	final public boolean isNullIV() {
//
////        return (counter == 0 && hash == 0);
//        return false;
//
//	}
//
//	/**
//	 * Singleton for a "null" {@link IV}.
//	 */
//	final public static transient BlobIV<?> NullIV = BlobIV.mockIV(VTE.URI);
//
//	/**
//	 * Create a mock {@link IV} having the indicated {@link VTE} which will
//	 * report <code>true</code> for {@link #isNullIV()}. This is used by some
//	 * code patterns where we need to associate a {@link BigdataValue} not in
//	 * the database with an {@link IV} on a temporary basis.
//	 * 
//	 * @param vte
//	 *            The {@link VTE}.
//	 *            
//	 * @return The mock {@link IV}.
//	 */
//    static public BlobIV<?> mockIV(final VTE vte) {
//
//    	/*
//    	 * Note: This MUST be consistent with TermsIndexHelper#makeKey() !!!
//    	 */
//    	
//        return new BlobIV(vte, 0/* hashCode */, (short) 0/* counter */);
//
//	}

    /**
     * The hash code component of the key.
     */
    private final int hash;

    /**
     * The collision counter component of the key.
     */
    private final short counter;
    
    public IV<V, Void> clone(final boolean clearCache) {

        final BlobIV<V> tmp = new BlobIV<V>(flags, hash, counter);

        if (!clearCache) {

            tmp.setValue(getValueCache());
            
        }
        
        return tmp;

    }

    /**
	 * @param vte
	 *            The {@link VTE}.
	 * @param hash
	 *            The hash code.
	 * @param counter
	 *            The counter.
	 */
	public BlobIV(final VTE vte, final int hash, final short counter) {

		this(BlobIV.toFlags(vte), hash, counter);

	}

    /**
	 * @param flags
	 *            The flags byte.
	 * @param hash
	 *            The hash code.
	 * @param counter
	 *            The counter.
	 */
	public BlobIV(final byte flags, final int hash, final short counter) {

		super(flags);

		this.hash = hash;

		this.counter = counter;

	}

	/**
	 * Human readable representation includes the term identifier, whether this
	 * is a URI, Literal, Blank node, or Statement identifier and the datatype
	 * URI if one is assigned. <code>
	 * TermId(<i>hashCode</i>:<i>counter</i>:[U|L|B])
	 * </code>
	 * 
	 * @see IVUtility#fromString(String), which parses the string value returned
	 *      by this method.
	 */
    public String toString() {
    	
		return "BlobIV(" + hashCode() + ":" + counter() + ":"
				+ getVTE().getCharCode() + ")";

    }

    /**
     * Decodes the output of {@link #toString()}, returning a new {@link BlobIV}
     * .
     * 
     * @param s
     *            The string representation.
     * 
     * @return The {@link BlobIV}.
     */
    public static BlobIV<?> fromString(final String s) {

        final char type = s.charAt(s.length() - 2);

        final VTE vte = VTE.valueOf(type);

		// everything after the '(' up to (but not including) the VTE type code.
		final String t = s.substring(7/* beginIndex */, s.length() - 3);
		
		// the marker between the hash code and the counter.
		final int pos = t.indexOf(':');

		if (pos == -1)
            throw new RuntimeException("Not a BlobIV: [" + s + "]");
		
		final String hstr = t.substring(0, pos);

		final String cstr = t.substring(pos + 1);

		final int hashCode = Integer.valueOf(hstr);

		final int counter = Integer.valueOf(cstr);

	    @SuppressWarnings("unchecked")
		final BlobIV<?> tmp = new BlobIV(vte, hashCode, (short) counter);
		
		return tmp;
		
	}
    
//    /**
//     * Callers must explicitly populate the value cache.
//     * <p>
//     * {@inheritDoc}
//     */
//    @Override
//    final public V setValue(V v) {
//    	
//    	return super.setValue(v);
//    	
//    }
//    
//    /**
//     * Operation is not supported because this {@link IV} type is not 100%
//     * inline. You MUST explicitly set the value cache.
//     * <p>
//     * {@inheritDoc}
//     * 
//     * @see #setValue(BigdataValue)
//     */
//    final public V asValue(final LexiconRelation lex) {
//        throw new UnsupportedOperationException();
//    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof BlobIV<?>) {
        	final BlobIV<?> t = (BlobIV<?>)o;
        	if(this.hash!=t.hash) return false;
        	if(this.counter!=t.counter) return false;
        	if(this.flags()!=t.flags()) return false;
        	return true;
//			return BytesUtil.bytesEqual(data, ((TermId<?>) o).data);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * <p>
     * @return The hash code component of the key in the TERMS index.
     */
    @Override
    final public int hashCode() {

        return hash;
        
    }

    /**
     * The collision counter component of the key in the TERMS index.
     */
    final public int counter() {

    	return counter;
    	
    }
    
    final public int byteLength() {

        return BlobsIndexHelper.TERMS_INDEX_KEY_SIZE;
        
    }

    @Override
    public int _compareTo(final IV o) {

    	final BlobIV<?> t = (BlobIV<?>) o;
    	
        // TODO Must impose correct order. Sign extension on the flags might
        // make that order wrong.
		if( flags < t.flags ) return -1;
		if( flags > t.flags ) return 1;
		if(hash<t.hash) return -1;
		if(hash>t.hash) return 1;
		if(counter<t.counter) return -1;
		if(counter>t.counter) return 1;
		return 0;

//		return UnsignedByteArrayComparator.INSTANCE.compare(data,
//				((TermId<?>) o).data);
		
//        final long termId2 = ((TermId<?>) o).termId;
//        
//        return termId < termId2 ? -1 : termId > termId2 ? 1 : 0; 
        
    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to entirely take over the encoding of the key from the
	 * {@link BlobIV}. Note that this simply copies the private unsigned byte[]
	 * into the {@link IKeyBuilder} since it is already the exact key for the
	 * {@link BlobIV}.
	 */
    @Override
    final public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

    	/*
    	 * Note: This MUST be consistent with TermsIndexHelper#makeKey() !!!
    	 */
        keyBuilder.appendSigned(flags);
        keyBuilder.appendSigned(INonInlineExtensionCodes.BlobIV);
        keyBuilder.append(hash);
        keyBuilder.append(counter);
    	
        return keyBuilder;
        
    }

//	/**
//	 * Override default serialization to send the cached {@link BigdataValue}.
//	 */
//	private void writeObject(final java.io.ObjectOutputStream out)
//			throws IOException {
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
//	private void readObject(final java.io.ObjectInputStream in)
//			throws IOException, ClassNotFoundException {
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

    /**
     * {@inheritDoc}
     * <p>
     * Creates a unique blank node ID based on the {@link BlobIV}'s internal
     * data.
     */
    @Override
    public String getID() {

        if (!isBNode())
            throw new ClassCastException();

        final long id = ((long) flags()) << 56/* hash:int + counter:short */
                | ((long) hash) << 16/* short */| counter;

        final String idStr = Long.toString(id);

        return idStr;

    }

    @Override
    final public byte getExtensionByte() {
     
        return INonInlineExtensionCodes.BlobIV;
        
    }
    
    public IV getExtensionIV() {
    	
    	return null;
    	
    }
    
}
