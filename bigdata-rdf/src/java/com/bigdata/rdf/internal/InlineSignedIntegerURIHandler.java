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

	public InlineSignedIntegerURIHandler(String namespace) {
		super(namespace);
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected AbstractLiteralIV createInlineIV(String localName) {
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
	 */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(BigInteger value) {
		if (value.compareTo(MIN_LONG_AS_BIGINT) < 0 || value.compareTo(MAX_LONG_AS_BIGINT) > 0) {
			return new XSDIntegerIV(value);
		}
		return createInlineIV(value.longValue());
	}

	/**
	 * Create the smallest AbstractLiteralIV that will fit the provided value.
	 */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(long value) {
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
