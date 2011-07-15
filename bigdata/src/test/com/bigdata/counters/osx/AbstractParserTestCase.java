package com.bigdata.counters.osx;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

public class AbstractParserTestCase extends TestCase2 {

	public AbstractParserTestCase() {
	}

	public AbstractParserTestCase(String name) {
		super(name);
	}

    /**
     * Used to verify that the header corresponds to our expectations. Logs
     * errors when the expectations are not met.
     * 
     * @param header
     *            The header line.
     * @param fields
     *            The fields parsed from that header.
     * @param field
     *            The field number in [0:#fields-1].
     * @param expected
     *            The expected value of the header for that field.
     */
    static protected void assertField(final String header,
            final String[] fields, final int field, final String expected) {

        if (header == null)
            throw new IllegalArgumentException();

        if (fields == null)
            throw new IllegalArgumentException();

        if (expected == null)
            throw new IllegalArgumentException();

        if (field < 0)
            throw new IllegalArgumentException();

        if (field >= fields.length)
            throw new AssertionFailedError("There are only " + fields.length
                    + " fields, but field=" + field + "\n" + header);

        if (!expected.equals(fields[field])) {

            throw new AssertionFailedError("Expected field=" + field
                    + " to be [" + expected + "], actual=" + fields[field]
                    + "\n" + header);

        }

    }

}
