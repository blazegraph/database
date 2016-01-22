package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IBindingSet;
import com.bigdata.striterator.CloseableIteratorWrapper;

import cutthecrap.utils.striterators.SingleValueIterator;

/**
 * Test suite for {@link SolutionSetStatserator}
 * 
 * @author bryan
 */
public class TestSolutionSetStatserator extends
		AbstractSolutionSetStatsTestCase {

	public TestSolutionSetStatserator() {
	}

	public TestSolutionSetStatserator(String name) {
		super(name);
	}

	@Override
	protected ISolutionSetStats newFixture(final IBindingSet[] bindingSets) {

		final SolutionSetStatserator itr = new SolutionSetStatserator(
				new CloseableIteratorWrapper<IBindingSet[]>(
						new SingleValueIterator<IBindingSet[]>(bindingSets)));
		try {
			while (itr.hasNext()) {
				// statistics filter will observed each solution set[].
				itr.next();
			}
			// Return the compiled statistics.
			return itr.getStats();
		} finally {
			itr.close();
		}
	}

    /**
     * Correct rejection test for the constructor.
     */
    public void test_001() {
        
        try {
            new SolutionSetStatserator(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

}
