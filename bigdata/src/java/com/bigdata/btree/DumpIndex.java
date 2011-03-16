package com.bigdata.btree;

/**
 * Utility class to dump an index in a variety of ways.
 * 
 * @author thompsonbry
 */
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

	/**
	 * Dumps pages (nodes and leaves) using a low-level approach.
	 * 
	 * @param ndx
	 *            The index.
	 * @param dumpNodeState
	 *            When <code>true</code>, provide gruesome detail on each
	 *            visited page.
	 */
	public static void dumpPages(final AbstractBTree ndx,
			final boolean dumpNodeState) {

		final PageStats stats = new PageStats();
		
		dumpPages(ndx, ndx.getRoot(), stats, dumpNodeState);

		System.out.println("name=" + ndx.getIndexMetadata().getName() + " : "
				+ stats);
		
	}
	
	/*
	 * FIXME report the min/max page size as well and report as a nice table in
	 * DumpJournal.
	 */
	static class PageStats {
		
		/** The #of nodes visited. */
		public long nnodes;
		/** The #of leaves visited. */
		public long nleaves;
		/** The #of bytes in the raw records for the nodes visited. */
		public long nodeBytes;
		/** The #of bytes in the raw records for the leaves visited. */
		public long leafBytes;
		
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append(getClass().getName());
			sb.append("{nnodes="+nnodes);
			sb.append(",nleaves="+nleaves);
			sb.append(",nodeBytes="+nodeBytes);
			sb.append(",leafBytes="+leafBytes);
			sb.append(",bytesPerNode="+(nnodes==0?0:nodeBytes/nnodes));
			sb.append(",bytesPerLeaf="+(nleaves==0?0:leafBytes/nleaves));
			sb.append("}");
			return sb.toString();
		}
		
	}

	private static void dumpPages(final AbstractBTree ndx,
			final AbstractNode<?> node, final PageStats stats,
			final boolean dumpNodeState) {

		if (dumpNodeState)
			node.dump(System.out);

		final long addrSelf = node.getIdentity();
		
		final long nbytes = ndx.getStore().getByteCount(addrSelf);

		final boolean isLeaf = node.isLeaf();
		
		if (isLeaf) {

			stats.nleaves++;
			stats.leafBytes += nbytes;

		} else {

			stats.nnodes++;
			stats.nodeBytes += nbytes;

			final int nkeys = node.getKeyCount();

			for (int i = 0; i <= nkeys; i++) {

				// normal read following the node hierarchy, using cache, etc.
				final AbstractNode<?> child = ((Node) node).getChild(i);

				// recursive dump
				dumpPages(ndx, child, stats, dumpNodeState);

			}

		}

	}

}
