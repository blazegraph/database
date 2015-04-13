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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.vocab.Vocabulary;

/**
 * Default implementation of {@link IInlineURIFactory} that comes pre-loaded
 * with two handlers: IPv4 ({@link InlineIPv4URIHandler}) and UUID
 * ({@link InlineUUIDURIHandler}.
 */
public class InlineURIFactory implements IInlineURIFactory {
	private final Logger log = Logger.getLogger(InlineURIFactory.class);
    private final List<InlineURIHandler> handlers = 
            new LinkedList<InlineURIHandler>();
	private final Map<String, InlineURIHandler> handlersByNamespace = new HashMap<>();
    
    /**
     * By default, handle IPv4 and UUID.
     */
    public InlineURIFactory() {
        addHandler(new InlineUUIDURIHandler(InlineUUIDURIHandler.NAMESPACE));
        addHandler(new InlineIPv4URIHandler(InlineIPv4URIHandler.NAMESPACE));
    }
    
    protected void addHandler(final InlineURIHandler handler) {
        this.handlers.add(handler);
    }

    public void init(final Vocabulary vocab) {
        for (InlineURIHandler handler : handlers) {
            handler.init(vocab);
            handlersByNamespace.put(handler.getNamespace(), handler);
        }
    }
    
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public URIExtensionIV createInlineURIIV(URI uri) {
        for (InlineURIHandler handler : handlers) {
            final URIExtensionIV iv = handler.createInlineIV(uri);
            if (iv != null) {
                return iv;
            }
        }
        return null;
    }

	@Override
	public String getLocalNameFromDelegate(URI namespace,
			AbstractLiteralIV<BigdataLiteral, ?> delegate) {
		InlineURIHandler handler = handlersByNamespace.get(namespace
				.stringValue());
		if (handler == null) {
			/*
			 * Since we can't find the handler the default implementation is the
			 * best we can manage.
			 */
			log.warn("Couldn't find InlineURIHandler for "
					+ namespace.stringValue());
			return InlineURIHandler
					.getLocalNameFromDelegateDefaultImplementation(delegate);
		}
		return handler.getLocalNameFromDelegate(delegate);
	}
}
