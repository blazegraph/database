package com.bigdata.btree;

import com.bigdata.util.BytesUtil;

/**
 * Utility class to dump an index in a variety of ways.
 * 
 * @author thompsonbry
 * 
 *         FIXME GIST : Generalize this for non-B+Tree indices.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
 *      </a>
 * */
public class DumpIndex {

	/**
	 * Utility method using an {@link ITupleIterator} to dump the keys and
	 * values in an {@link AbstractBTree}.
	 * 
	 * @param ndx
	 *            The index.
	 * @param showTuples
	 *            When <code>true</code> the data for the keys and values will
	 *            be displayed. Otherwise the scan will simply exercise the
	 *            iterator.
	 */
	public static void dumpIndex(final AbstractBTree ndx,
			final boolean showTuples) {

		// Note: reused for each tuple to avoid heap churn.
		final StringBuilder sb = new StringBuilder();

		// @todo offer the version metadata also if the index supports
		// isolation.
		final ITupleIterator<?> itr = ndx.rangeIterator(null, null);

		final long begin = System.currentTimeMillis();

		int i = 0;

		while (itr.hasNext()) {

			final ITuple<?> tuple = itr.next();

			if (showTuples) {

				dumpTuple(i, sb, tuple);

			}

			i++;

		}

		final long elapsed = System.currentTimeMillis() - begin;

		System.out.println("Visited " + i + " tuples in " + elapsed + "ms");

	}

	private static void dumpTuple(final int recno, final StringBuilder tupleSB,
			final ITuple<?> tuple) {

		final ITupleSerializer<?, ?> tupleSer = tuple.getTupleSerializer();

		tupleSB.setLength(0); // reset.

		tupleSB.append("rec=" + recno);

		try {

			tupleSB.append("\nkey=" + tupleSer.deserializeKey(tuple));

		} catch (Throwable t) {

			tupleSB.append("\nkey=" + BytesUtil.toString(tuple.getKey()));

		}

		try {

			tupleSB.append("\nval=" + tupleSer.deserialize(tuple));

		} catch (Throwable t) {

			tupleSB.append("\nval=" + BytesUtil.toString(tuple.getValue()));

		}

		System.out.println(tupleSB);

	}

}
