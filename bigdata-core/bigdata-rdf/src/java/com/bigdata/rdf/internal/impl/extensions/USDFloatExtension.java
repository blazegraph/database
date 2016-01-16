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

package com.bigdata.rdf.internal.impl.extensions;

import java.util.Collections;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.vocab.decls.BSBMVocabularyDecl;

/**
 * Adds inlining for the
 * <code>http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/USD</code>
 * datatype, which is treated as <code>xsd:float</code>.
 */
@SuppressWarnings("rawtypes")
public class USDFloatExtension<V extends BigdataValue> implements IExtension<V> {

    private final BigdataURI datatype;
    
    public USDFloatExtension(final IDatatypeURIResolver resolver) {

        datatype = resolver.resolve(BSBMVocabularyDecl.USD);
        
    }
    
    public Set<BigdataURI> getDatatypes() {
        
        return Collections.singleton(datatype);
        
    }
    
    /**
     * Attempts to convert the supplied value into an internal representation
     * using BigInteger.
     */
    @SuppressWarnings("unchecked")
    public LiteralExtensionIV createIV(final Value value) {
        
        if (value instanceof Literal == false)
            throw new IllegalArgumentException();
        
        final Literal lit = (Literal) value;
        
        final AbstractLiteralIV delegate = new XSDNumericIV<BigdataLiteral>(
                XMLDatatypeUtil.parseFloat(lit.getLabel()));

        return new LiteralExtensionIV(delegate, datatype.getIV());
        
    }
    
    @SuppressWarnings("unchecked")
    public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {

        final String s = Float.toString(iv.getDelegate().floatValue());

        return (V) vf.createLiteral(s, datatype);

    }

}
