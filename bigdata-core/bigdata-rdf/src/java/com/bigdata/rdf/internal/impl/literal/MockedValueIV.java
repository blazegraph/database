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
 * Created on Dec 8, 2015
 */
package com.bigdata.rdf.internal.impl.literal;

import org.openrdf.model.Literal;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.DTE;
import com.bigdata.rdf.internal.DTEExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.util.BytesUtil;

/**
 * Internally used IV representing a mocked value. The IV contains a delegate,
 * which typically would be something like a fully inlined URI iv (or a fully
 * inlined literal/blank node, respectively). It is just a wrapper to indicate
 * that the delegate is to be translated back into a mocked value.
 * 
 * Note that this inherits from {@link AbstractLiteralIV}, but does not necessarily
 * reflect a literal: the reflected type depends on the inner type. We do not
 * expose this to the outside (i.e., there's no datatype that can be used for
 * constructing these literals), but this is for internal use only. See BLZG-611.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class MockedValueIV extends AbstractLiteralIV<BigdataLiteral, IV<?,?>> {

    private static final long serialVersionUID = 9136542087440805253L;

    /**
     * The delegate IV
     */
    private final IV<?,?> delegate;
    
    /**
     * Only used for compareTo() and byteLength().  Encoding takes place in
     * AbstractIV, decoding in IVUtility.
     */
    private transient byte[] key;
    
    /**
     * Cached hash code.
     */
    private transient int hashCode = 0;
    
    /**
     * Construct an instance using the delegate.
     * 
     * @param delegate the delegate
     */
    public MockedValueIV(final IV<?,?> delegate) {
        super(DTE.Extension);
        
        // only using one byte for the array length
        if (delegate == null) {
            throw new IllegalArgumentException();
        }
        
        this.delegate = delegate;
    }
    
    @Override 
    public DTEExtension getDTEX() {
        return DTEExtension.MOCKED_IV;
    }
    
    public IV<?,?> getIV() {
        return delegate;
    }
    
    private byte[] key() {
        if (key == null) {
            key = super.encode(KeyBuilder.newInstance()).getKey();
        }
        return key;
    }
    
    @Override
    public int byteLength() {
        return key().length;
    }

    @Override
    public MockedValueIV clone(boolean clearCache) {
        return new MockedValueIV(this.delegate);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int _compareTo(IV o) {
        if (!(o instanceof MockedValueIV)) {
            throw new IllegalArgumentException();
        }
        final MockedValueIV iv = (MockedValueIV) o;
        return BytesUtil.compareBytes(key(), iv.key());
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = delegate.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockedValueIV other = (MockedValueIV) obj;
        
        return obj.equals(other);
    }
    
    /**
     * Implement {@link Literal#getLabel()} for logging.  Superclass uses 
     * inline value.
     */
    @Override
    public String getLabel() {
        return "MockedIV["+delegate+"]";
    }

    /**
     * We could theoretically get all the inline values from the inline IVs
     * and return them here.
     */
    @Override
    public IV<?,?> getInlineValue() throws UnsupportedOperationException {
        throw new UnsupportedOperationException(); // not required
    }

    /**
     * This IV cannot be materialized on its own. 
     */
    @Override
    public BigdataLiteral asValue(LexiconRelation lex) throws UnsupportedOperationException {
        throw new UnsupportedOperationException(); // not required
    }

}
