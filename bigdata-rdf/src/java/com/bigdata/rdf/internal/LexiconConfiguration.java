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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;


/**
 * An object which describes which kinds of RDF Values are inlined into the
 * statement indices and how other RDF Values are coded into the lexicon.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo large literal size boundary.
 * @todo other configuration options.
 */
public class LexiconConfiguration implements ILexiconConfiguration {
    
    private boolean inlineLiterals, inlineBNodes;
    
    private Map<TermId, IExtension> termIds;
    
    private Map<String, IExtension> datatypes;
    
    public LexiconConfiguration(final boolean inlineLiterals, 
            final boolean inlineBNodes, 
            final IExtensionFactory xFactory) {
        this.inlineLiterals = inlineLiterals;
        this.inlineBNodes = inlineBNodes;
        
        termIds = new HashMap<TermId, IExtension>();
        datatypes = new HashMap<String, IExtension>();
        for (IExtension extension : xFactory.getExtensions()) {
            BigdataURI datatype = extension.getDatatype();
            if (datatype == null)
                continue;
            termIds.put((TermId) datatype.getIV(), extension);
            datatypes.put(datatype.stringValue(), extension);
        }
        
    }
    
    public Value asValue(final ExtensionIV iv, final BigdataValueFactory vf) {
        final TermId datatype = iv.getExtensionDatatype();
        return termIds.get(datatype).asValue(iv, vf);
    }
    
    public IV createInlineIV(final Value value) {

        // we know right away we can't handle URIs
        if (value instanceof URI)
            return null;
        
        if (value instanceof Literal) {
            
            final Literal l = (Literal) value;
            
            final URI datatype = l.getDatatype();
            
            // not a datatyped literal
            if (datatype == null)
                return null;

            if (datatypes.containsKey(datatype.stringValue())) {
                
                try {
                    
                    final IExtension xFactory = 
                        datatypes.get(datatype.stringValue());
                    
                    final IV iv = xFactory.createIV(value);
                    
                    if (iv != null && value instanceof BigdataValue)
                        ((BigdataValue) value).setIV(iv);
                    
                    return iv;
                    
                } catch (Exception ex) {
                    
                    /* 
                     * Some sort of parse error in the literal value most 
                     * likely. Resort to term identifiers. 
                     */
                    return null;
                    
                }
                
            }
            
            // get the native DTE
            final DTE dte = DTE.valueOf(datatype);
            
            // no native DTE for this datatype
            if (dte == null)
                return null;

            // check to see if we are inlining literals of this type
            if (!isInline(VTE.LITERAL, dte))
                return null;
            
            final String v = value.stringValue();
            
            IV iv = null;
            
            try {
                
                switch(dte) {
                    case XSDBoolean:
                        iv = new XSDBooleanIV(XMLDatatypeUtil.parseBoolean(v));
                        break;
                    case XSDByte:
                        iv = new XSDByteIV(XMLDatatypeUtil.parseByte(v));
                        break;
                    case XSDShort:
                        iv = new XSDShortIV(XMLDatatypeUtil.parseShort(v));
                        break;
                    case XSDInt:
                        iv = new XSDIntIV(XMLDatatypeUtil.parseInt(v));
                        break;
                    case XSDLong:
                        iv = new XSDLongIV(XMLDatatypeUtil.parseLong(v));
                        break;
                    case XSDFloat:
                        iv = new XSDFloatIV(XMLDatatypeUtil.parseFloat(v));
                        break;
                    case XSDDouble:
                        iv = new XSDDoubleIV(XMLDatatypeUtil.parseDouble(v));
                        break;
                    case XSDInteger:
                        iv = new XSDIntegerIV(XMLDatatypeUtil.parseInteger(v));
                        break;
                    case XSDDecimal:
                        iv = new XSDDecimalIV(XMLDatatypeUtil.parseDecimal(v));
                        break;
                    case UUID:
                        iv = new UUIDLiteralIV(UUID.fromString(v));
                        break;
                    default:
                        iv = null;
                }
                
            } catch (NumberFormatException ex) {
                
                // some dummy doesn't know how to format a number
                // default to term identifier for this term 
                
            }
            
            if (iv != null && value instanceof BigdataValue)
                ((BigdataValue) value).setIV(iv);
            
            return iv;
            
        } else if (value instanceof BNode) {
            
            final BNode b = (BNode) value;
            
            final String id = b.getID();
            
            final char c = id.charAt(0);
            
            try {

                final UUID uuid = UUID.fromString(id);
                
                if (!uuid.toString().equals(id))
                    return null;
                
                if (!isInline(VTE.BNODE, DTE.UUID))
                    return null;
                
                final IV iv = new UUIDBNodeIV(uuid);
                
                if (value instanceof BigdataValue)
                    ((BigdataValue) value).setIV(iv);
                
                return iv;
                
            } catch (Exception ex) {
                
                // string id could not be converted to a UUID
                
            }
            
            try {

                final Integer i = Integer.valueOf(id);
                
                // cannot normalize id, needs to remain syntactically identical
                if (!i.toString().equals(id))
                    return null;
                
                if (!isInline(VTE.BNODE, DTE.XSDInt))
                    return null;
                
                final IV iv = new NumericBNodeIV(i);
                
                if (value instanceof BigdataValue)
                    ((BigdataValue) value).setIV(iv);
                
                return iv;
                
            } catch (Exception ex) {
                
                // string id could not be converted to an Integer
                
            }
            
        }
        
        return null;
        
    }
    
    /**
     * See {@link ILexiconConfiguration#isInline(VTE, DTE)}.
     */
    public boolean isInline(final VTE vte, final DTE dte) {
        
        switch(vte) {
        case BNODE:
            return inlineBNodes && isSupported(dte);
        case LITERAL:
            return inlineLiterals && isSupported(dte);
        default:
            return false;
        }
        
    }
    
    private boolean isSupported(DTE dte) {
        
        switch (dte) {
        case XSDBoolean:
        case XSDByte:
        case XSDShort:
        case XSDInt:
        case XSDLong:
        case XSDFloat:
        case XSDDouble:
        case XSDInteger:
        case XSDDecimal:
        case UUID:
            return true;
        case XSDUnsignedByte:       // none of the unsigneds are tested yet
        case XSDUnsignedShort:      // none of the unsigneds are tested yet
        case XSDUnsignedInt:        // none of the unsigneds are tested yet
        case XSDUnsignedLong:       // none of the unsigneds are tested yet
        default:
            return false;
        }
        
    }

}
