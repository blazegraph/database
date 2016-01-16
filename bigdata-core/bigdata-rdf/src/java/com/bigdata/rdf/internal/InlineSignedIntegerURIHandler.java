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

import java.math.BigInteger;

import org.apache.log4j.Logger;

import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;

/**
 * Simple InlineURIHandler that packs the localName portion of the URI into an
 * appropriately sized signed integer.
 */
public class InlineSignedIntegerURIHandler extends InlineURIHandler {
	private static final Logger log = Logger
			.getLogger(InlineSignedIntegerURIHandler.class);
	private static final BigInteger MIN_LONG_AS_BIGINT = BigInteger
			.valueOf(Long.MIN_VALUE);
	private static final BigInteger MAX_LONG_AS_BIGINT = BigInteger
			.valueOf(Long.MAX_VALUE);

	public InlineSignedIntegerURIHandler(final String namespace) {
		super(namespace);
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(final String localName) {
		BigInteger value;
		try {
			value = new BigInteger(localName, 10);
		} catch (NumberFormatException e) {
			if (log.isDebugEnabled()) {
				log.debug("Invalid integer", e);
			}
			return null;
		}
		return createInlineIV(value);
	}

    /**
     * Create the smallest AbstractLiteralIV that will fit the provided value.
     * Public and static so it can be easily used as a building block for other
     * InlineURIHandlders.
     */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(final BigInteger value) {
		if (value.compareTo(MIN_LONG_AS_BIGINT) < 0 || value.compareTo(MAX_LONG_AS_BIGINT) > 0) {
			return new XSDIntegerIV(value);
		}
		return createInlineIV(value.longValue());
	}

    /**
     * Create the smallest AbstractLiteralIV that will fit the provided value.
     * Public and static so it can be easily used as a building block for other
     * InlineURIHandlders.
     */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(final long value) {
		if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
			return new XSDNumericIV((byte) value);
		}
		if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
			return new XSDNumericIV((short) value);
		}
		if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
			return new XSDNumericIV((int) value);
		}
		return new XSDNumericIV(value);
	}
}
