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
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;

/**
 * Simple InlineURIHandler that packs the localName portion of the URI into an
 * appropriately sized signed integer.
 */
public class InlineUnsignedIntegerURIHandler extends InlineURIHandler {
	private static final Logger log = Logger
			.getLogger(InlineUnsignedIntegerURIHandler.class);
	private static final BigInteger MAX_UNSIGNED_LONG_AS_BIGINT = new BigInteger(
			"18446744073709551616");

	public InlineUnsignedIntegerURIHandler(final String namespace) {
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
		if (value.compareTo(BigInteger.ZERO) <= 0) {
			return null;
		}
		return createInlineIV(value);
	}

    /**
     * Create the smallest AbstractLiteralIV that will fit the provided value.
     * Public and static so it can be easily used as a building block for other
     * InlineURIHandlders.
     * 
     * @param value a positive integer
     */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(final BigInteger value) {
		if (value.compareTo(MAX_UNSIGNED_LONG_AS_BIGINT) >= 0) {
			return new XSDIntegerIV(value);
		}
		return createInlineIV(value.longValue());
	}

    /**
     * Create the smallest AbstractLiteralIV that will fit the provided
     * value.Public and static so it can be easily used as a building block for
     * other InlineURIHandlders.
     * 
     * @param value a positive integer
     */
    @SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(final long value) {
    	if (value < 0L) {
			return new XSDUnsignedLongIV(value + Long.MIN_VALUE);
		}
		if (value < 256L) {
			return new XSDUnsignedByteIV((byte) (value + Byte.MIN_VALUE));
		}
		if (value < 65536L) {
			return new XSDUnsignedShortIV((short) (value + Short.MIN_VALUE));
		}
		if (value < 4294967296L) {
			return new XSDUnsignedIntIV((int) (value + Integer.MIN_VALUE));
		}
		return new XSDUnsignedLongIV(value + Long.MIN_VALUE);
	}
}
