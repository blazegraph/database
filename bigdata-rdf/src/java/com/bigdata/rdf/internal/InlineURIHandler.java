/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Handler is mapped to a namespace prefix.  When a URI is presented that
 * matches the handler's namespace prefix, attempt to parse the remaining
 * portion of the URI into an inline literal.  The namespace prefix must be
 * present in the vocabulary.  The localName must be parseable into an inline
 * literal.  If either of these things is not true the URI will not be inlined.
 */
public abstract class InlineURIHandler {

    /**
     * The namespace prefix.
     */
    protected final String namespace;
    
    /**
     * Namespace prefix length.
     */
    protected final int len;
    
    /**
     * The inline vocab IV for the namespace prefix.
     */
    @SuppressWarnings("rawtypes")
    protected transient IV namespaceIV;
    
    /**
     * Create a handler for the supplied namespace prefix.
     */
    public InlineURIHandler(final String namespace) {
        this.namespace = namespace;
        this.len = namespace.length();
    }
    
    /**
     * Lookup the namespace IV from the vocabulary.
     */
    public void init(final Vocabulary vocab) {
        this.namespaceIV = vocab.get(new URIImpl(namespace));
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected URIExtensionIV createInlineIV(final URI uri) {
        
        /*
         * If the namspace prefix is not in the vocabulary we can't inline 
         * anything.
         */
        if (namespaceIV == null) {
            return null;
        }
        
        if (uri.stringValue().startsWith(namespace)) {
            final String localName = uri.stringValue().substring(len);
            final AbstractLiteralIV localNameIV = createInlineIV(localName);
            if (localNameIV != null) {
                return new URIExtensionIV(localNameIV, namespaceIV);
            }
        }
        
        return null;
    }
    
    /**
     * Concrete subclasses are responsible for actually creating the inline
     * literal IV for the localName.
     */
    @SuppressWarnings("rawtypes")
    protected abstract AbstractLiteralIV createInlineIV(final String localName);
    
}
