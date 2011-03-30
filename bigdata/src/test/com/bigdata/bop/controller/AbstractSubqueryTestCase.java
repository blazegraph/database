package com.bigdata.bop.controller;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.ThickAsynchronousIterator;
import com.bigdata.striterator.ICloseableIterator;

import junit.framework.TestCase2;

/**
 * Abstract base class for subquery join test suites.
 * 
 * @author thompsonbry
 */
abstract public class AbstractSubqueryTestCase extends TestCase2 {

	public AbstractSubqueryTestCase() {
	}

	public AbstractSubqueryTestCase(String name) {
		super(name);
	}


    /**
     * Return an {@link IAsynchronousIterator} that will read a single,
     * empty {@link IBindingSet}.
     * 
     * @param bindingSet
     *            the binding set.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet bindingSet) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { new IBindingSet[] { bindingSet } });

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSets
     *            the binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[] bindingSets) {

        return new ThickAsynchronousIterator<IBindingSet[]>(
                new IBindingSet[][] { bindingSets });

    }

    /**
     * Return an {@link IAsynchronousIterator} that will read a single, chunk
     * containing all of the specified {@link IBindingSet}s.
     * 
     * @param bindingSetChunks
     *            the chunks of binding sets.
     */
    protected ThickAsynchronousIterator<IBindingSet[]> newBindingSetIterator(
            final IBindingSet[][] bindingSetChunks) {

        return new ThickAsynchronousIterator<IBindingSet[]>(bindingSetChunks);

    }

    /**
     * Verify the expected solutions.
     * 
     * @param expected
     * @param itr
     */
    static public void assertSameSolutions(final IBindingSet[] expected,
            final IAsynchronousIterator<IBindingSet[]> itr) {
        try {
            int n = 0;
            while (itr.hasNext()) {
                final IBindingSet[] e = itr.next();
                if (log.isInfoEnabled())
                    log.info(n + " : chunkSize=" + e.length);
                for (int i = 0; i < e.length; i++) {
                    if (log.isInfoEnabled())
                        log.info(n + " : " + e[i]);
                    if (n >= expected.length) {
                        fail("Willing to deliver too many solutions: n=" + n
                                + " : " + e[i]);
                    }
                    if (!expected[n].equals(e[i])) {
                        fail("n=" + n + ", expected=" + expected[n]
                                + ", actual=" + e[i]);
                    }
                    n++;
                }
            }
            assertEquals("Wrong number of solutions", expected.length, n);
        } finally {
            itr.close();
        }
    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public <T> void assertSameSolutionsAnyOrder(final T[] expected,
            final Iterator<T> actual) {

        assertSameSolutionsAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     * <p>
     * Note: If the objects being visited do not correctly implement hashCode()
     * and equals() then this can fail even if the desired objects would be
     * visited. When this happens, fix the implementation classes.
     */
    static public <T> void assertSameSolutionsAnyOrder(final String msg,
            final T[] expected, final Iterator<T> actual) {

        try {

            /*
             * Populate a map that we will use to realize the match and
             * selection without replacement logic. The map uses counters to
             * handle duplicate keys. This makes it possible to write tests in
             * which two or more binding sets which are "equal" appear.
             */

            final int nrange = expected.length;

            final java.util.Map<T, AtomicInteger> range = new java.util.LinkedHashMap<T, AtomicInteger>();

            for (int j = 0; j < nrange; j++) {

                AtomicInteger count = range.get(expected[j]);

                if (count == null) {

                    count = new AtomicInteger();

                }

                range.put(expected[j], count);

                count.incrementAndGet();
                
            }

            // Do selection without replacement for the objects visited by
            // iterator.

            for (int j = 0; j < nrange; j++) {

                if (!actual.hasNext()) {

                    fail(msg
                            + ": Iterator exhausted while expecting more object(s)"
                            + ": index=" + j);

                }

                final T actualObject = actual.next();

                if (log.isInfoEnabled())
                    log.info("visting: " + actualObject);

                AtomicInteger counter = range.get(actualObject);

                if (counter == null || counter.get() == 0) {

                    fail("Object not expected" + ": index=" + j + ", object="
                            + actualObject);

                }

                counter.decrementAndGet();
                
            }

            if (actual.hasNext()) {

                final List<T> remainder = new LinkedList<T>(); 
                
                while(actual.hasNext()) {
                    remainder.add(actual.next());
                }

                fail("Iterator will deliver too many objects: reminder("
                        + remainder.size() + ")=" + remainder);

            }

        } finally {

            if (actual instanceof ICloseableIterator<?>) {

                ((ICloseableIterator<T>) actual).close();

            }

        }

    }

}
