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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Utility to provide a map of multiple inline URI handlers for a single
 * namespace.  This is used in conjunction with the {@link InlineLocalNameIntegerURIHandler}. 
 * 
 * Currently, up to 32 different URI handlers are supported for a given namespace.
 * 
 * For an example, see {@see com.blazegraph.vocabulary.pubchem.PubChemInlineURIFactory}.
 * 
 * @author beebs
 *
 */
public class InlineIntegerURIHandlerMap extends InlineLocalNameIntegerURIHandler {
	
	private static final Logger log = Logger.getLogger(InlineIntegerURIHandlerMap.class);
	
	//Precompile Pattern to pick out the prefix and/or suffix around an integer
	public static final Pattern descriptorPattern = Pattern.compile("(.*[a-zA-Z])\\d+(_.*)");

	//Map of IDs to Handlers
	private HashMap<Integer,InlineURIHandler> hMap = new HashMap<Integer,InlineURIHandler>();
	
	//Treemap of Namespaces and URIs
	private final TreeMap<String, InlineURIHandler> handlersByLocalName = new TreeMap<String, InlineURIHandler>();

	
	public InlineIntegerURIHandlerMap(final String namespace) {
		super(namespace);
	}
	
	public void addHandlerForNS(final int id, final InlineURIHandler handler) {
		
		assert(handler != null);
		
		final Integer key = new Integer(id);
		
		final InlineURIHandler h = hMap.get(key);
		
		if(h != null) {
			log.warn("Handler " + h.getClass().getName()
					+ " already registered for id: " + id
					+ ".  Overriding with a new value.");
		}
		
		hMap.put(key,handler);
		
		handlersByLocalName.put(getKeyForHandler(handler), handler);
		
	}

	 /**
     * Unpack the inline value into the localName portion of the uri.
     * 
     * unpack the ID encoded in the value and select the correct handler to use.
     * 
     */
	public String getLocalNameFromDelegate(final AbstractLiteralIV<BigdataLiteral, ?> delegate) {
		
		final String str = delegate.getInlineValue().toString();

		final Integer key = new Integer(unpackId(str));
		
		if(!hMap.containsKey(key)) {
			
			throw new RuntimeException(key + " decoded from " + str + " for "
					+ getNamespace() + " is not registered.");
			
		}
		
		final InlineURIHandler h = hMap.get(key);

    	return h.getLocalNameFromDelegate(delegate);
    	
    }

	/**
	 * Select the best Integer Handler.  Uses the same logic as {@link InlineURIFactory}.
	 * 
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractLiteralIV createInlineIV(final String localName) {

			Matcher m = descriptorPattern.matcher(localName);
			
			String str = "";
			
			if(m.matches()) {
			
				if(m.end()>1) { //Prefix AND Suffix

					str+=m.group(1) + m.group(2);

				} else if(m.end(1) > 0) { //Prefix OR Suffix

					str+=m.group(1);

				} else {
					log.warn("No localname match found for " + localName);
				}
			} 
			
			// Find handler with longest prefix match LTE the given URI.
			final Map.Entry<String, InlineURIHandler> floorEntry = handlersByLocalName.floorEntry(str);

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

				final AbstractLiteralIV iv = handler.createInlineIV(localName);

				if (iv != null) {

					return iv;

				}

			}

			return null;
			
			
	}
	
	private String getKeyForHandler(final InlineURIHandler handler) {
		
		//TODO: This only handles prefixes, suffixes and combinations.
		String key = "";
		
		if(handler instanceof IPrefixedURIHandler) {
			key += ((IPrefixedURIHandler)handler).getPrefix();
		}
		
		if(handler instanceof ISuffixedURIHandler) {
			key += ((ISuffixedURIHandler)handler).getSuffix();
		}
		
		return key;
		
	}

}
