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

import java.util.Map;
import java.util.TreeMap;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Default implementation of {@link IInlineURIFactory} that comes pre-loaded
 * with two handlers:
 * <ul>
 * <li>IPv4 ({@link InlineIPv4URIHandler})</li>
 * <li>UUID ({@link InlineUUIDURIHandler}</li>
 * </ul>
 * You MAY declare additional handlers. This MUST be done before the triple
 * store instance has been created. Once the triple store exists, the
 * translation imposed by the lexicon configuration and the lexicon indices must
 * be stable.
 * 
 * @see BLZG-1507 (Implement support for DTE extension types for URIs)
 */
public class InlineURIFactory implements IInlineURIFactory {
    
//	private final List<InlineURIHandler> handlers = new LinkedList<InlineURIHandler>();

	/**
	 * The set of declared {@link InlineURIHandler}s, with an index on the
	 * prefix for the handler.
	 * <p>
	 * Note: I've removed the separate linked list of the handlers and changed
	 * this to a tree map. This provides lookup by prefix and the longest prefix
	 * match is now used rather than visiting all registered handlers.
	 * <p>
	 * This map should be populated in the constructor for this class and
	 * the subclasses in order to guarantee that the changes are visible
	 * once we leave the scope of the constructor.
	 */
	private final TreeMap<String, InlineURIHandler> handlersByNamespace = new TreeMap<String, InlineURIHandler>();
    
    /**
     * By default, handle IPv4 and UUID.
     */
    public InlineURIFactory() {

    	addHandler(new InlineUUIDURIHandler(InlineUUIDURIHandler.NAMESPACE));
        
    	addHandler(new InlineIPv4URIHandler(InlineIPv4URIHandler.NAMESPACE));
    	
    }
    
	/**
	 * Declare a handler. This must be invoked in the constructor for this class
	 * or the constructor of a subclass. The set of registered handlers must not
	 * be changed outside of the constructor scope both for reasons of
	 * visibility of the changes (thread-safety) and stability of the mapping of
	 * {@link URI}s onto {@link IV}s.
	 * 
	 * @param handler
	 */
    protected void addHandler(final InlineURIHandler handler) {

    	//        this.handlers.add(handler);
        getHandlersByNamespace().put(handler.getNamespace(), handler);
        
    }

    @Override
    public void init(final Vocabulary vocab) {

    	for (InlineURIHandler handler : getHandlersByNamespace().values()) {
        
    		handler.init(vocab);
    		
        }
    	
    }

    @Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public URIExtensionIV createInlineURIIV(final URI uri) {

		final String str = uri.stringValue();

		// Find handler with longest prefix match LTE the given URI.
		final Map.Entry<String, InlineURIHandler> floorEntry = getHandlersByNamespace().floorEntry(str);

		if (floorEntry == null) {

    		// No potentially suitable handler.
    		return null;
 
		}

		final String prefix = floorEntry.getKey();
		
		/*
		 * Note: the floorEntry is NOT guaranteed to be a prefix. It can also be
		 * strictly LT the probe key. Therefore we must additionally verify here
		 * that the prefix under which the URI handler was registered is a
		 * prefix of the URI before invoking that handler.
		 */
		if (str.startsWith(prefix)) {

			final InlineURIHandler handler = floorEntry.getValue();

			final URIExtensionIV iv = handler.createInlineIV(uri);

			if (iv != null) {

				return iv;

			}

		}

		return null;

		/*
		 * Note: This code checks each handler. This is presumably being done
		 * because the ipv4 syntax can include "/" characters (for the optional
		 * netmask) so the URI namespace (as defined by openrdf) is not matching
		 * the prefix under which the handler is registered.
		 */
//        for (InlineURIHandler handler : handlersByNamespace.values()) {
//            final URIExtensionIV iv = handler.createInlineIV(uri);
//            if (iv != null) {
//                return iv;
//            }
//        }
//        return null;
    }

    @Override
    public String getLocalNameFromDelegate(final URI namespace,
            final AbstractLiteralIV<BigdataLiteral, ?> delegate) {

		final InlineURIHandler handler = getHandlersByNamespace().get(namespace.stringValue());
		
		if (handler == null) {
			throw new IllegalArgumentException(
					"Can't resolve uri handler for \"" + namespace + "\".  Maybe its be deregistered?");
		}
		
		return handler.getLocalNameFromDelegate(delegate);
		
	}

	private TreeMap<String, InlineURIHandler> getHandlersByNamespace() {
		return handlersByNamespace;
	}

}
