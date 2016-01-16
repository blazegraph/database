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
package com.bigdata.rdf.internal;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Handler is mapped to a namespace prefix. When a URI is presented that matches
 * the handler's namespace prefix, this class attempts to parse the remaining
 * portion of the URI into an inline literal. The namespace prefix must be
 * present in the vocabulary. The localName must be parseable into an inline
 * literal. If either of these things is not true the URI will not be inlined.
 */
public abstract class InlineURIHandler {

	private static final Logger log = Logger.getLogger(InlineURIHandler.class);
	
    /**
     * The namespace prefix.
     */
    private final String namespace;

    /**
     * Namespace prefix length.
     */
    private final int len;

    /**
     * The inline vocab IV for the namespace prefix.
     */
    @SuppressWarnings("rawtypes")
    protected transient IV namespaceIV;

    /**
	 * Create a handler for the supplied namespace prefix - the handler will be
	 * invoked iff it is the registered handler having the longest prefix LTE to
	 * the actual URI.
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
		
    	if (namespaceIV == null)
			log.warn("No vocabulary entry for namespace - URIs with this namespace will not be inlined: namespace="
					+ namespace);
    	
	}

    /**
     * The namespace this handles. Used for resolving the handler after load so
     * it can inflate the localName portion of the inlined uri.
     */
    public String getNamespace() {
        return namespace;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected URIExtensionIV createInlineIV(final URI uri) {

        /*
         * If the namespace prefix is not in the vocabulary we can't inline
         * anything.
         */
		if (namespaceIV == null) {
			return null;
		}

		/*
		 * Note: Removed test. This is guaranteed by the caller.
		 */
//		if (uri.stringValue().startsWith(namespace)) {

		final String localName = uri.stringValue().substring(len);

		final AbstractLiteralIV localNameIV = createInlineIV(localName);

		if (localNameIV != null) {

			return new URIExtensionIV(localNameIV, namespaceIV);

		}

//		}

		return null;
		
	}

    /**
     * Unpack the inline value into the localName portion of the uri.
     */
	public String getLocalNameFromDelegate(final AbstractLiteralIV<BigdataLiteral, ?> delegate) {

    	return delegate.getInlineValue().toString();
    	
    }

    /**
     * Concrete subclasses are responsible for actually creating the inline
     * literal IV for the localName.
     */
    @SuppressWarnings("rawtypes")
    protected abstract AbstractLiteralIV createInlineIV(final String localName);

}
