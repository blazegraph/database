package com.bigdata.btree;

import java.util.Stack;

import junit.framework.AssertionFailedError;

import com.bigdata.btree.DefaultEvictionListener;
import com.bigdata.btree.PO;
import com.bigdata.cache.HardReferenceQueue;

/**
 * Extends {@link DefaultEvictionListener} to explicitly control when an
 * eviction notice is expected and to verify that eviction notices are received
 * as expected.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MyEvictionListener extends
        DefaultEvictionListener {

    /**
     * Set the next N expected references for eviction notices.  You can
     * only do this when nothing is currently expected.
     * 
     * @param refs
     *            The expected references.
     * 
     * @exception IllegalStateExecption
     *                unless there is no current expected reference.
     */
    public void setExpectedRefs(PO[] refs) {

        if (expectedRef != null) {

            throw new IllegalStateException();

        }

        assert refs != null;

        assert refs.length > 0;

        for (int i = refs.length - 1; i >= 0; i--) {

            PO ref = refs[i];

            assert ref != null;

            expectedRefs.push(ref);

        }

        setExpectedRef(expectedRefs.pop());

    }

    Stack<PO> expectedRefs = new Stack<PO>();

    /**
     * Set the expected reference for the next eviction notice. The listener
     * will thrown an exception if there is a cache eviction unless you
     * first invoke this method.
     * 
     * @param ref
     *            The expected reference or null to cause the listener to
     *            throw an exception if a reference is evicted.
     */
    public void setExpectedRef(PO ref) {

        this.expectedRef = ref;

        this.evicted = false;

    }

    private PO expectedRef = null;

    /**
     * Test for an eviction event.
     * 
     * @exception AssertionFailedError
     *                if nothing was evicted since the last time an expected
     *                eviction reference was set.
     */
    public void assertEvicted() {

        if (!evicted) {

            TestCopyOnWrite.fail("Expected " + expectedRef + " to have been evicted.");

        }

    }

    private boolean evicted = false;

    /**
     * Test for the expected #of eviction notices to date.
     * 
     * @param expected
     */
    public void assertEvictionCount(int expected) {

        TestCopyOnWrite.assertEquals("evictionCount", expected, nevicted);

    }

    /**
     * The #of eviction notices to date.
     */
    public int getEvictionCount() {
        return nevicted;
    }

    private int nevicted = 0;

    /**
     * @throws AssertionFailedError
     *             if the evicted reference is not the next expected
     *             eviction reference or if no eviction is expected.
     */
    public void evicted(HardReferenceQueue<PO> queue, PO ref) {

        TestCopyOnWrite.assertNotNull("queue", queue);
        TestCopyOnWrite.assertNotNull("ref", ref);

        if (expectedRef == null && expectedRefs.size() > 0) {

            /*
             * There is no current expectation, but there is one on the
             * stack, so we pop it off the stack and continue.
             * 
             * Note: We pop the expectation off of the stack lazily so that
             * the unit tests have the opportunity to verify that an
             * expected reference was evicted.
             */
            setExpectedRef(expectedRefs.pop());

        }

        if (expectedRef == null) {

            TestCopyOnWrite.fail("Not expecting an eviction: ref=" + ref);

        }

        TestCopyOnWrite.assertEquals("ref", expectedRef, ref); // Note: This is a reference test.
        //                assertTrue("ref", expectedRef == ref);

        // Reset the expectated ref to null.
        expectedRef = null;

        // Note that the eviction occurred.
        evicted = true;

        nevicted++;

        // Delegate the actual eviction behavior.
        super.evicted(queue, ref);

    }

}
