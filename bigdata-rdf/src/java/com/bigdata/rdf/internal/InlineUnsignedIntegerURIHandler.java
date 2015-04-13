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

	public InlineUnsignedIntegerURIHandler(String namespace) {
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
		if (value.compareTo(BigInteger.ZERO) <= 0) {
			return null;
		}
		return createInlineIV(value);
	}

	/**
	 * Create the smallest AbstractLiteralIV that will fit the provided value.
	 * 
	 * @param value
	 *            a positive integer
	 */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(BigInteger value) {
		if (value.compareTo(MAX_UNSIGNED_LONG_AS_BIGINT) >= 0) {
			return new XSDIntegerIV(value);
		}
		return createInlineIV(value.longValue());
	}

	/**
	 * Create the smallest AbstractLiteralIV that will fit the provided value.
	 * 
	 * @param value
	 *            a positive integer
	 */
	@SuppressWarnings("rawtypes")
	public static AbstractLiteralIV createInlineIV(long value) {
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
