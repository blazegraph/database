/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.ap;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.IPredicate.Annotations;
import com.bigdata.bop.ap.SampleIndex.AcceptanceSetOffsetSampler;
import com.bigdata.bop.ap.SampleIndex.IOffsetSampler;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.ap.SampleIndex.SmartOffsetSampler;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.striterator.ChunkedArrayIterator;

/**
 * Test suite for {@link SampleIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestSampleLocalBTree.java 3665 2010-09-28 16:53:22Z thompsonbry
 *          $
 * 
 *          FIXME Just like {@link TestPredicateAccessPath}, this test suite
 *          needs to cover all of the combinations of global views of
 *          partitioned and unpartitioned indices.
 */
public class TestSampleIndex extends TestCase2 {

    /**
     * 
     */
    public TestSampleIndex() {
    }

    /**
     * @param name
     */
    public TestSampleIndex(String name) {
        super(name);
    }
    
    @Override
    public Properties getProperties() {

        final Properties p = new Properties(super.getProperties());

        p.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Transient
                .toString());

        return p;
        
    }

    static private final String namespace = "ns";
    
    Journal jnl;
    
    R rel;
    
    public void setUp() throws Exception {
        
        jnl = new Journal(getProperties());

    }
    
    /**
     * Create and populate relation in the {@link #namespace}.
     * 
     * @return The #of distinct entries.
     */
    private int loadData(final int scale) {

		final String[] names = new String[] { "John", "Mary", "Saul", "Paul",
				"Leon", "Jane", "Mike", "Mark", "Jill", "Jake", "Alex", "Lucy" };

		final Random rnd = new Random();
		
		// #of distinct instances of each name.
		final int populationSize = Math.max(10, (int) Math.ceil(scale / 10.));
		
		// #of trailing zeros for each name.
		final int nzeros = 1 + (int) Math.ceil(Math.log10(populationSize));
		
//		System.out.println("scale=" + scale + ", populationSize="
//				+ populationSize + ", nzeros=" + nzeros);

		final NumberFormat fmt = NumberFormat.getIntegerInstance();
		fmt.setMinimumIntegerDigits(nzeros);
		fmt.setMaximumIntegerDigits(nzeros);
		fmt.setGroupingUsed(false);
		
        // create the relation.
        final R rel = new R(jnl, namespace, ITx.UNISOLATED, new Properties());
        rel.create();

        // data to insert.
		final E[] a = new E[scale];

		for (int i = 0; i < scale; i++) {

			final String n1 = names[rnd.nextInt(names.length)]
					+ fmt.format(rnd.nextInt(populationSize));

			final String n2 = names[rnd.nextInt(names.length)]
					+ fmt.format(rnd.nextInt(populationSize));

//			System.err.println("i=" + i + ", n1=" + n1 + ", n2=" + n2);
			
			a[i] = new E(n1, n2);
			
        }

		// sort before insert for efficiency.
		Arrays.sort(a,R.primaryKeyOrder.getComparator());
		
        // insert data (the records are not pre-sorted).
        final long ninserts = rel.insert(new ChunkedArrayIterator<E>(a.length, a, null/* keyOrder */));

        // Do commit since not scale-out.
        jnl.commit();

        // should exist as of the last commit point.
        this.rel = (R) jnl.getResourceLocator().locate(namespace,
                ITx.READ_COMMITTED);

        assertNotNull(rel);

        return (int) ninserts;
        
    }

    public void tearDown() throws Exception {

        if (jnl != null) {
            jnl.destroy();
            jnl = null;
        }
        
        // clear reference.
        rel = null;

    }

	/**
	 * Stress test for {@link IOffsetSampler}s.
	 * 
	 * TODO Look at the distributions of the different {@link IOffsetSampler}s.
	 * They should be uniform.
	 */
	public void test_offsetSamplers() {

		// Note: Only handles a special case!
//		new GetOffsetsEntireRange(),
		
		final IOffsetSampler[] samplers = new IOffsetSampler[] {
				new SmartOffsetSampler(), //
//				new BitVectorOffsetSampler(),//
				new AcceptanceSetOffsetSampler(),//
//				new RejectionSetOffsetSampler(), //
				};

		final Random r = new Random();
		
		final int ntrials = 1000;
		
		for (int trial = 0; trial < ntrials; trial++) {

			// 10% seed is 0L (which gets turned into random anyway)
			final long seed = r.nextDouble() < .1 ? 0 : r.nextLong();

			final int entryCount = r.nextInt(100000);
			
			// 10% fromIndex is zero.
			final int fromIndex = r.nextDouble() < .1 ? 0 : r
					.nextInt(entryCount);

			final int remaining = entryCount - fromIndex;

			final int toIndex = r.nextDouble() < .1 ? entryCount : (fromIndex
					+ r.nextInt(remaining) + 1);

			final int rangeCount = toIndex - fromIndex;
			
			final int limit = r.nextDouble() < .1 ? r.nextInt(100) + 1 : r
					.nextDouble() < .5 ? r.nextInt(entryCount) + 1 : r
					.nextInt(10000) + 1;
			
			for (IOffsetSampler sampler : samplers) {

				try {

					final long begin = System.currentTimeMillis(); 
					
					final int[] offsets = sampler.getOffsets(seed, limit, fromIndex, toIndex);
					
					final long elapsed = System.currentTimeMillis() - begin;
					
					if (elapsed > 1000) {
						log.warn("Slow: elapsed=" + elapsed + ", class="
								+ sampler.getClass() + ", seed=" + seed
								+ ", limit=" + limit + ", fromIndex="
								+ fromIndex + ",toIndex=" + toIndex);
					}

					// check the #of offsets returned.
					final int noffsets = offsets.length;
					assertTrue(noffsets <= limit);
					if (limit > rangeCount)
						assertTrue(noffsets <= rangeCount);
					else 
						assertTrue(noffsets == limit);

					// check offsets ordered, within range, and w/o dups.
					int lastOffset = -1;
					for (int j = 0; j < offsets.length; j++) {

						final int offset = offsets[j];

						if (offset < fromIndex)
							fail("index=" + j
									+ ", offset LT fromIndex: offset=" + offset
									+ ", fromIndex=" + fromIndex);

						if (offset >= toIndex)
							fail("index=" + j + ", offset GTE toIndex: offset="
									+ offset + ", toIndex=" + toIndex);

						if (offset <= lastOffset) {
							fail("index=" + j + ", lastOffset=" + lastOffset
									+ ", but offset=" + offset);
						}
						
						lastOffset = offset;

					}
					
				} catch (Throwable t) {

					fail("sampler=" + sampler.getClass() + ", seed=" + seed
							+ ", limit=" + limit + ", fromIndex=" + fromIndex
							+ ",toIndex=" + toIndex + ", rangeCount="
							+ rangeCount, t);

				}

			}

		}

	}

    /**
	 * Unit test verifies some aspects of a sample taken from a local index
	 * (primarily that the sample respects the limit).
	 * 
	 * @todo test when the range count is zero.
	 * 
	 * @todo test when the inclusive lower bound of a key range is an insertion
	 *       point (no tuple for that key).
	 * 
	 * @todo test when the exclusive upper bound of a key range is an insertion
	 *       point (no tuple for that key).
	 */
    public void test_something() {

    	final int scale = 10000;
    	
        final int nrecords = loadData(scale);
        
        final IVariable<?> x = Var.var("x");

		final IVariable<?> y = Var.var("y");

		final IPredicate<E> predicate = new Predicate<E>(new BOp[] { x, y },
				new NV(IPredicate.Annotations.RELATION_NAME,
						new String[] { namespace }),//
				new NV(Annotations.TIMESTAMP, ITx.READ_COMMITTED)//
		);

		final int[] limits = new int[] { //
		1, 9, 19, 100, 217, 900,// 
		nrecords, 
		nrecords + 1
		};

		for (SampleType sampleType : SampleType.values()) {

			if (log.isInfoEnabled())
				log.info("Testing: SampleType=" + sampleType);

			for (int limit : limits) {

				doTest(nrecords, limit, sampleType, predicate);

			}

		}

	}
    
	private void doTest(final int nrecords, final int limit,
			final SampleType sampleType, final IPredicate<E> predicate) {

		final BOpContextBase context = new BOpContextBase(null/* fed */, jnl/* indexManager */);

		final SampleIndex<E> sampleOp = new SampleIndex<E>( new BOp[0], //
				NV.asMap(//
					new NV(SampleIndex.Annotations.PREDICATE, predicate),//
					new NV(SampleIndex.Annotations.LIMIT, limit),//
					new NV(SampleIndex.Annotations.SAMPLE_TYPE, sampleType
							.name())//
				));

		final E[] a = sampleOp.eval(context);

		if (log.isInfoEnabled()) {

			System.err.println("limit=" + limit + ", nrecords=" + nrecords
					+ ", nsamples=" + a.length + ", sampleType=" + sampleType);

			for (int i = 0; i < a.length && i < 10; i++) {
			
				System.err.println("a[" + i + "]=" + a[i]);
				
			}
			
		}

		final int nexpected = Math.min(nrecords, limit);

		assertEquals("#samples (limit=" + limit + ", nrecords=" + nrecords
				+ ", nexpected=" + nexpected + ")", nexpected, a.length);
    }

}
