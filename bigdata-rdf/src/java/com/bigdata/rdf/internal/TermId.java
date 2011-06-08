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

import java.io.IOException;
import java.math.BigInteger;

import org.openrdf.model.Value;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.constraints.DatatypeBOp;
import com.bigdata.rdf.internal.constraints.FuncBOp;
import com.bigdata.rdf.internal.constraints.LangBOp;
import com.bigdata.rdf.lexicon.TermsIndexHelper;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataEvaluationStrategyImpl3;

/**
 * Implementation for any kind of RDF Value when the values is not being
 * inlined. Instances of this class can represent URIs, Blank Nodes (if they are
 * not being inlined), or Literals (including datatype literals if they are not
 * being inlined). The representation of the {@link TermId} is simply the key in
 * the TERMS index for the tuple for a given RDF {@link Value}. While flexible,
 * {@link Value}s models as {@link TermId}s require indirection through the
 * TERMS index in order to materialize the {@link Value}. We are often able to
 * inline {@link Value}s compactly and efficiently into the statement indices,
 * in which case those values simply do not appear in the TERMS index. The
 * {@link TermId} remains useful when the RDF {@link Value} is large and not
 * otherwise amenable to inlining.
 */
public class TermId<V extends BigdataValue> extends
        AbstractNonInlineIV<V, Void> {

    /**
     * 
     */
//    private static final long serialVersionUID = 4309045651680610931L;
    private static final long serialVersionUID = 2;
    
    /**
     * Return the <code>flags</code> byte for a {@link TermId}. 
     */
	public static final byte toFlags(final VTE vte) {
		/*
		 * Note: XSDBoolean happens to be assigned the code value of 0, which is
		 * the value we want when the data type enumeration will be ignored.
		 */
		return AbstractIV.toFlags(vte, false/* inline */,
				false/* extension */, DTE.XSDBoolean);
	}
    
//    /**
//     * Value used for a "NULL" term identifier.
//     */
//    public static final transient long NULL = 0L;
//
//    /** The term identifier. */
//    private final long termId;

	/**
	 * {@inheritDoc}
	 * <p>
	 * This checks the hashCode and the counter, both of which must decode to
	 * ZERO (0). The {@link VTE} may be any of the possible {@link VTE}s. "null"
	 * versions of different kinds of {@link VTE}s are created by
	 * {@link #mockIV(VTE)}.
	 * 
	 * @see LangBOp
	 * @see DatatypeBOp
	 * @see FuncBOp
	 * @see BigdataEvaluationStrategyImpl3
	 * @see DummyIV
	 */
    @Override
	final public boolean isNullIV() {

		return counter() == 0 && hashCode() == 0;
//		for (int i = 1; i < data.length; i++) {
//
//			if (data[i] != 0)
//				return false;
//
//		}
//
//		return true;

	}

	/**
	 * Note: This key has a ZERO hashCode and a ZERO counter. It also uses
	 * {@link VTE#URI}, which is a ZERO (byte). However, when we encode the
	 * hashCode and the counter, the zeros get converted from signed to
	 * unsigned.
	 */
    static private final byte[] null_key;
    static {

		final TermsIndexHelper h = new TermsIndexHelper();

		final IKeyBuilder keyBuilder = h.newKeyBuilder();

		null_key = h
				.makeKey(keyBuilder, VTE.URI, 0/* hashCode */, 0/* counter */);

    }

	/**
	 * Singleton for a "null" {@link IV}.
	 */
	final public static transient TermId<?> NullIV = new TermId(null_key);

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
    static public TermId mockIV(final VTE vte) {

//    	// start with the NullIV's key.
//    	final TermId mockIV = new TermId(null_key.clone());
//		
//    	// override the flags byte.
//    	mockIV.data[0] = KeyBuilder.encodeByte(toFlags(vte));

		final TermsIndexHelper h = new TermsIndexHelper();

		final IKeyBuilder keyBuilder = h.newKeyBuilder();
		
		final byte[] key = h
				.makeKey(keyBuilder, vte, 0/* hashCode */, 0/* counter */);
    	
		final TermId mockIV = new TermId(key);
		
    	// return the mock IV.
		return mockIV;
		
	}
    
    /**
	 * The unsigned byte[] key for the TERMS index. The first byte is the
	 * <em>flags</em>. This is followed by a hashCode and a counter. The counter
	 * is used to break ties in the collision buckets formed by all
	 * {@link BigdataValue} inserted into the TERMS index which have the same
	 * flags and hashCode.
	 */
	private final byte[] data;

//    /**
//     * Constructor for a term identifier when you are decoding and already have
//     * the flags.
//     * 
//     * @param flags
//     *            The flags
//     * @param termId
//     * 
//     * @deprecated This is an old termId variant.
//     */
//    public TermId(final byte flags, final long termId) {
//
//        super(flags);
//
////        this.termId = termId;
//
//        throw new UnsupportedOperationException();
////        this.dataTypeId = 0L;
//        
//    }

    /**
     * Constructor for a term identifier.
     * 
     * @param vte
     * @param termId
     * 
     * @deprecated This is an old termId variant.
     */
    public TermId(final VTE vte, final long termId) {

		super(toFlags(vte));

//        this.termId = termId;
       
        throw new UnsupportedOperationException();
        
    }
    
    public TermId(final byte[] data) {

		super(data[0]/* flags */);
    	
		this.data = data;
    	
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
    	
		return "TermId(" + hashCode() + ":" + counter() + ":"
				+ getVTE().getCharCode() + ")";

    }

	/**
	 * Decodes the output of {@link #toString()}, returning a new {@link TermId}
	 * .
	 * 
	 * @param s
	 *            The string representation.
	 *            
	 * @return The {@link TermId}.
	 */
    public static TermId fromString(final String s) {

    	final char type = s.charAt(s.length() - 2);
    	
    	final VTE vte = VTE.valueOf(type);
    	
		// everything after the '(' up to (but not including) the VTE type code.
		final String t = s.substring(7/* beginIndex */, s.length() - 3);
		
		// the marker between the hash code and the counter.
		final int pos = t.indexOf(':');

		if (pos == -1)
			throw new RuntimeException(s);
		
		final String hstr = t.substring(0, pos);

		final String cstr = t.substring(pos + 1);

		final int hashCode = Integer.valueOf(hstr);

		final int counter = Integer.valueOf(cstr);

		final TermsIndexHelper helper = new TermsIndexHelper();
		
		final IKeyBuilder keyBuilder = helper.newKeyBuilder();

		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode, counter);
		
		return new TermId(key);

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
//    
//    final public Void getInlineValue() {
//        throw new UnsupportedOperationException();
//    }

    final public long getTermId() {
//        return termId;
    	throw new UnsupportedOperationException();
    }
    
//    /**
//     * Always returns <code>false</code> since the RDF value is not inline.
//     */
//    @Override
//    final public boolean isInline() {
//        return false;
//    }

    /**
     * Always returns <code>true</code> since this is a term identifier.
     */
    @Override
    final public boolean isTermId() {
        return true;
    }

//    /**
//     * {@inheritDoc
//     * 
//     * Note: only the termId matters for equality (unless we also have a
//     * transient reference to the value factory which stands in as a proxy for
//     * the KB instance).
//     */
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof TermId<?>) {
			return BytesUtil.bytesEqual(data, ((TermId<?>) o).data);
//            return termId == ((TermId<?>) o).termId;
        }
        return false;
    }

    /**
     * Return the hash code component of the key in the TERMS index.
     */
    public int hashCode() {
//        return (int) (termId ^ (termId >>> 32));
		if (hash == 0) {
			hash = KeyBuilder.decodeInt(data, 1/* off */);
		}
		return hash;
    }
    private int hash = 0;

    /**
     * The hash collision counter.
     */
    public int counter() {

    	final byte b = data[data.length - 1];
    	
    	return KeyBuilder.decodeByte(b);
//    	return b;
    	
    }
    
    public int byteLength() {
    	return data.length;
//        return 1/* flags */+ Bytes.SIZEOF_LONG;
        
    }

    @Override
    protected int _compareTo(final IV o) {

		return UnsignedByteArrayComparator.INSTANCE.compare(data,
				((TermId<?>) o).data);
		
//        final long termId2 = ((TermId<?>) o).termId;
//        
//        return termId < termId2 ? -1 : termId > termId2 ? 1 : 0; 
        
    }

	/**
	 * {@inheritDoc}
	 * <p>
	 * Overridden to entirely take over the encoding of the key from the
	 * {@link TermId}. Note that this simply copies the private unsigned byte[]
	 * into the {@link IKeyBuilder} since it is already the exact key for the
	 * {@link TermId}.
	 */
    @Override
    final public IKeyBuilder encode(final IKeyBuilder keyBuilder) {

//		/*
//		 * Assuming that we store the byte[] where we used to store the
//		 * termId this just becomes keyBuilder.append(data).
//		 */
//
//        // First emit the flags byte.
//        keyBuilder.appendSigned(flags());
//
//        keyBuilder.append(getTermId());

    	keyBuilder.append(data);
    	
        return keyBuilder;
        
    }

	/**
	 * Override default serialization to send the cached {@link BigdataValue}.
	 */
	private void writeObject(final java.io.ObjectOutputStream out)
			throws IOException {

		out.defaultWriteObject();

		out.writeObject(getValueCache());

	}

	/**
	 * Override default serialization to recover the cached {@link BigdataValue}
	 * .
	 */
	@SuppressWarnings("unchecked")
	private void readObject(final java.io.ObjectInputStream in)
			throws IOException, ClassNotFoundException {

		in.defaultReadObject();

		final V v = (V) in.readObject();

		if (v != null) {
			// set the value cache.
			setValue(v);
		}

	}

    /**
     * Using the {@link BigInteger} class to create a unique bnode id based on
     * the byte[] key of the {@link TermId}.
     */
    public String bnodeId() {
        
        final int signum = data.length > 0 ? 1 : 0;
        
        final BigInteger bi = new BigInteger(signum, data);
        
        return "B" + bi.toString();
        
    }

}
