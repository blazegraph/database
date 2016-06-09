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

import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;

/**
 * 
 * A container URIHandler that handles multiple inline URI possibilities for a
 * given namespace. The handler searches registered namespaces and finds the a
 * match for a given URI. All of the handlers must have the same namespace
 * prefix.
 * 
 * For example: <code> 
 *   http://blazegraph.com/blzg/Data#Position_010072F0000038090100000000D56C9E
 * 	 http://blazegraph.com/blzg/Data#Position_010072F0000038090100000000D56C9E_TaxCost
 * </code>
 * 
 * Each of these can be supported as a different {@link InlineHexUUIDURIHandler}
 * {@link InlineSuffixedHexUUIDURIHandler} at the namespace
 * http://www.blazegraph.com/blzg/Data#Position_.
 * 
 * Examples of how to configure Hex-encoded UUID based URIs for inlining using 
 * this handler. You may also do this with integers with prefixes, suffixes, 
 * or a combination.
 * 
 * Each namespace inlined must have a corresponding vocabulary declaration.
 * 
 * <code>
 *		InlineNamespaceMultiURIHandler mHandler = new InlineNamespaceMultiURIHandler(
 *				"http://blazegraph.com/Data#Position_");
 *
 *		mHandler.addHandler(new InlineSuffixedHexUUIDURIHandler(
 *				"http://blazegraph.com/Data#Position_", "_TaxCost"));
 *
 *		mHandler.addHandler(new InlineSuffixedHexUUIDURIHandler(
 *				"http://blazegraph.com/Data#Position_", "_UnrealizedGain"));
 *
 *		mHandler.addHandler(new InlineSuffixedHexUUIDURIHandler(
 *				"http://blazegraph.com/Data#Position_", "_WashSale"));
 *
 *		mHandler.addHandler(new InlineHexUUIDURIHandler(
 *				"http://blazegraph.com/Data#Position_"));
 *
 *		this.addHandler(mHandler);
 *		</code>
 * 
 * {@link https://jira.blazegraph.com/browse/BLZG-1938}
 * 
 * @author beebs
 *
 */
public class InlineNamespaceMultiURIHandler extends InlineURIHandler {

	private final List<InlineURIHandler> inlineHandlers = new LinkedList<InlineURIHandler>();

	public InlineNamespaceMultiURIHandler(String namespace) {
		super(namespace);
	}

	/**
	 * 
	 * Adds a new {@InlineURIHandler} for the namespace. The
	 * namespace of the handler must match that of the instance.
	 * 
	 * @param handler
	 *            Handler to add.
	 * @throws MultiNamespaceException
	 */
	public void addHandler(final InlineURIHandler handler) {

		// Check precondition of the same namespace
		if (!getNamespace().equals(handler.getNamespace())) {
			throw new RuntimeException("Tring to add " + handler.getNamespace()
					+ " to " + getClass().getCanonicalName()
					+ " configured for " + this.getNamespace());
		} else {
			inlineHandlers.add(handler);
		}

	}

	/**
	 * 
	 * Find the first handler of those register that successfully creates an
	 * inline value for the given localName.
	 * 
	 * @param localName
	 */
	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractLiteralIV createInlineIV(String localName) {

		/*
		 * {@link https://jira.blazegraph.com/browse/BLZG-1938}
		 */
		for (InlineURIHandler handler : inlineHandlers) {

			final AbstractLiteralIV iv = handler.createInlineIV(localName);

			if (iv != null) {

				return iv;

			}
		}

		return null;
	}

}
