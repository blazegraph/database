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
 * Created on Mar 5, 2012
 */

package com.bigdata.rdf.sail.webapp.client;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.parser.sparql.SPARQLUtil;

/**
 * Utility class for externalizing SPARQL prefix declaration management.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AST2SPARQLUtil {

    /**
     * The prefix declarations used within the SERVICE clause (from the original
     * query).
     */
    private final Map<String, String> prefixDecls;

    /** Reverse map for {@link #prefixDecls}. */
    private final Map<String, String> namespaces;

    public AST2SPARQLUtil(final Map<String, String> prefixDecls) {

        this.prefixDecls = prefixDecls;
        
        if (prefixDecls != null) {

            /*
             * Build up a reverse map from namespace to prefix.
             */
            
            namespaces = new HashMap<String, String>();

            for (Map.Entry<String, String> e : prefixDecls.entrySet()) {
   
                namespaces.put(e.getValue(), e.getKey());
                
            }
            
        } else {
            
            namespaces = null;
            
        }

    }
    
    /**
     * Return an external form for the {@link Value} suitable for direct
     * embedding into a SPARQL query.
     * 
     * @param val
     *            The value.
     * 
     * @return The external form.
     */
    public String toExternal(final Value val) {
        
        if (val instanceof URI) {

            return toExternal((URI) val);
        
        } else if (val instanceof Literal) {
        
            return toExternal((Literal)val);
            
        } else if (val instanceof BNode) {

            return toExternal((BNode)val);
            
        } else {
            
            throw new AssertionError();
            
        }

    }
    
    public String toExternal(final BNode bnd) {

        final String id = bnd.stringValue();

//        final boolean isLetter = Character.isLetter(id.charAt(0));

//        return "_:" + (isLetter ? "" : "B") + id;
        return "_:B" + id;

    }
    
    public String toExternal(final URI uri) {

        if (prefixDecls != null) {

            final String prefix = namespaces.get(uri.getNamespace());

            if (prefix != null) {

                return prefix + ":" + uri.getLocalName();

            }

        }

        return "<" + uri.stringValue() + ">";

    }
    
    public String toExternal(final Literal lit) {

        final String label = lit.getLabel();
        
        final String languageCode = lit.getLanguage();
        
        final URI datatypeURI = lit.getDatatype();

        final String datatypeStr = datatypeURI == null ? null
                : toExternal(datatypeURI);

        final StringBuilder sb = new StringBuilder((label.length() + 2)
                + (languageCode != null ? (languageCode.length() + 1) : 0)
                + (datatypeURI != null ? datatypeStr.length() + 2 : 0));

        sb.append('"');
        sb.append(SPARQLUtil.encodeString(label));
        sb.append('"');

        if (languageCode != null) {
            sb.append('@');
            sb.append(languageCode);
        } else {
            if (datatypeURI != null && !XMLSchema.STRING.equals(datatypeURI)) {
                sb.append("^^");
                sb.append(datatypeStr);
            }
        }
        return sb.toString();

    }

}
