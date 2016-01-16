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
/*
 * Created on Apr 27, 2010
 */

package com.bigdata.io;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import junit.framework.TestCase2;

import com.bigdata.journal.TestHelper;
import com.bigdata.util.BytesUtil;

/**
 * Base class for some <code>assertEquals</code> methods not covered by
 * {@link TestCase} or {@link TestCase2}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCase3 extends TestCase2 {

    
    /**
     * A random number generated - the seed is NOT fixed.
     */
    protected Random r;

    /**
     * 
     */
    public TestCase3() {
     
    }

    /**
     * @param name
     */
    public TestCase3(final String name) {
        super(name);
     
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        r = new Random();

    }

    @Override
    protected void tearDown() throws Exception {

        super.tearDown();

        r = null;
        
        TestHelper.checkJournalsClosed(this);
        
    }
    
    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public static void assertEquals(ByteBuffer expectedBuffer,
            final ByteBuffer actual) {

        if (expectedBuffer == null)
            throw new IllegalArgumentException();

        if (actual == null)
            fail("actual is null");

        if (expectedBuffer.hasArray() && expectedBuffer.arrayOffset() == 0
                && expectedBuffer.position() == 0
                && expectedBuffer.limit() == expectedBuffer.capacity()) {

            // evaluate byte[] against actual.
            assertEquals(expectedBuffer.array(), actual);

            return;

        }
        
        /*
         * Copy the expected data into a byte[] using a read-only view on the
         * buffer so that we do not mess with its position, mark, or limit.
         */
        final byte[] expected;
        {

            expectedBuffer = expectedBuffer.asReadOnlyBuffer();

            final int len = expectedBuffer.remaining();

            expected = new byte[len];

            expectedBuffer.get(expected);

        }

        // evaluate byte[] against actual.
        assertEquals(expected, actual);

    }

    /**
     * Helper method verifies that the contents of <i>actual</i> from
     * position() to limit() are consistent with the expected byte[]. A
     * read-only view of <i>actual</i> is used to avoid side effects on the
     * position, mark or limit properties of the buffer.
     * 
     * @param expected
     *            Non-null byte[].
     * @param actual
     *            Buffer.
     */
    public static void assertEquals(final byte[] expected, ByteBuffer actual) {

        if (expected == null)
            throw new IllegalArgumentException();

        if (actual == null)
            fail("actual is null");

        if (actual.hasArray() && actual.arrayOffset() == 0
                && actual.position() == 0
                && actual.limit() == actual.capacity()) {

            assertEquals(expected, actual.array());

            return;

        }

        /*
         * Create a read-only view on the buffer so that we do not mess with its
         * position, mark, or limit.
         */
        actual = actual.asReadOnlyBuffer();

        final int len = actual.remaining();

        final byte[] actual2 = new byte[len];

        actual.get(actual2);

        // compare byte[]s.
        assertEquals(expected, actual2);

    }

	/**
	 * Return the data in the buffer.
	 */
	public static byte[] getBytes(final ByteBuffer buf) {

	    return BytesUtil.getBytes(buf);

	}

    
    /**
     * Wait up to a timeout until some condition succeeds.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * @param timeout
     *            The timeout.
     * @param unit
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     */
    static public void assertCondition(final Runnable cond,
            final long timeout, final TimeUnit units) {
        final long begin = System.nanoTime();
        final long nanos = units.toNanos(timeout);
        long remaining = nanos;
        // remaining = nanos - (now - begin) [aka elapsed]
        remaining = nanos - (System.nanoTime() - begin);
        while (true) {
            try {
                // try the condition
                cond.run();
                // success.
                return;
            } catch (final Throwable e) {
//                final boolean interesting = //
//                InnerCause.isInnerCause(e, AssertionFailedError.class) || //
//                        InnerCause.isInnerCause(e, TimeoutException.class);
                remaining = nanos - (System.nanoTime() - begin);
                if (remaining < 0) {
                    // Timeout - rethrow the failed assertion.
                    throw new RuntimeException(e);
                }
                // Sleep up to 10ms or the remaining nanos, which ever is less.
                final int millis = (int) Math.min(
                        TimeUnit.NANOSECONDS.toMillis(remaining), 10);
                if (millis > 0) {
                    // sleep and retry.
                    try {
                        Thread.sleep(millis);
                    } catch (InterruptedException e1) {
                        // propagate the interrupt.
                        Thread.currentThread().interrupt();
                        return;
                    }
                    remaining = nanos - (System.nanoTime() - begin);
                    if (remaining < 0) {
                        // Timeout - rethrow the failed assertion.
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /**
     * Waits up to 5 seconds for the condition to succeed.
     * 
     * @param cond
     *            The condition, which must throw an
     *            {@link AssertionFailedError} if it does not succeed.
     * 
     * @throws AssertionFailedError
     *             if the condition does not succeed within the timeout.
     * 
     * @see #assertCondition(Runnable, long, TimeUnit)
     */
    static public void assertCondition(final Runnable cond) {
        
        assertCondition(cond, 5, TimeUnit.SECONDS);
        
    }

    /**
     * Returns random data that will fit in <i>nbytes</i>.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
     *         having random contents.
     */
    protected ByteBuffer getRandomData(final int nbytes) {

        final byte[] bytes = new byte[nbytes];

        r.nextBytes(bytes);

        return ByteBuffer.wrap(bytes);

    }

    /**
     * Returns random data that will fit in <i>nbytes</i>.
     * 
     * @return A new {@link ByteBuffer} wrapping a new <code>byte[]</code>
     *         having random contents.
     */
    protected ByteBuffer getRandomData(final ByteBuffer b, final int nbytes) {

        final byte[] a = new byte[nbytes];

        r.nextBytes(a);
        
        b.limit(nbytes);
        b.position(0);
        b.put(a);
        
        b.flip();
        
        return b;

    }

    /**
     * Return an open port on current machine. Try the suggested port first. If
     * suggestedPort is zero, just select a random port
     */
    protected static int getPort(int suggestedPort) throws IOException {

        ServerSocket openSocket;
        try {
            openSocket = new ServerSocket(suggestedPort);
        } catch (BindException ex) {
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        }

        final int port = openSocket.getLocalPort();

        openSocket.close();

        if (suggestedPort != 0 && port != suggestedPort) {

            log.warn("suggestedPort is busy: suggestedPort=" + suggestedPort + ", using port=" + port + " instead");

        }

        return port;

    }

}
