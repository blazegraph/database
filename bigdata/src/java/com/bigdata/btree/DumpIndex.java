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
	 * 
	 * @return Some interesting statistics about the pages in that index which
	 *         the caller can print out.
	 */
	public static PageStats dumpPages(final AbstractBTree ndx,
			final boolean dumpNodeState) {

		final PageStats stats = new PageStats();
		
		dumpPages(ndx, ndx.getRoot(), stats, dumpNodeState);

		return stats;
		
	}

	/**
	 * Class reports various summary statistics for nodes and leaves.
	 */
	public static class PageStats {
		
		/** The #of nodes visited. */
		public long nnodes;
		/** The #of leaves visited. */
		public long nleaves;
		/** The #of bytes in the raw records for the nodes visited. */
		public long nodeBytes;
		/** The #of bytes in the raw records for the leaves visited. */
		public long leafBytes;
		/** The min/max bytes per node. */
		public long minNodeBytes, maxNodeBytes;
		/** The min/max bytes per leaf. */
		public long minLeafBytes, maxLeafBytes;
		
		/** Return {@link #nodeBytes} plus {@link #leafBytes}. */
		public long getTotalBytes() {
			return nodeBytes + leafBytes;
		}

		/** The average bytes per node. */
		public long getBytesPerNode() {
			return (nnodes == 0 ? 0 : nodeBytes / nnodes);
		}

		/** The average bytes per leaf. */
		public long getBytesPerLeaf() {
			return (nleaves == 0 ? 0 : leafBytes / nleaves);
		}
		
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append(getClass().getName());
			sb.append("{nnodes=" + nnodes);
			sb.append(",nleaves=" + nleaves);
			sb.append(",nodeBytes=" + nodeBytes);
			sb.append(",minNodeBytes=" + minNodeBytes);
			sb.append(",maxNodeBytes=" + maxNodeBytes);
			sb.append(",leafBytes=" + leafBytes);
			sb.append(",minLeafBytes=" + minLeafBytes);
			sb.append(",maxLeafBytes=" + maxLeafBytes);
			sb.append(",bytesPerNode=" + getBytesPerNode());
			sb.append(",bytesPerLeaf=" + getBytesPerLeaf());
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
			if (stats.minLeafBytes > nbytes || stats.minLeafBytes == 0)
				stats.minLeafBytes = nbytes;
			if (stats.maxLeafBytes < nbytes)
				stats.maxLeafBytes = nbytes;

		} else {

			stats.nnodes++;
			stats.nodeBytes += nbytes;
			if (stats.minNodeBytes > nbytes || stats.minNodeBytes == 0)
				stats.minNodeBytes = nbytes;
			if (stats.maxNodeBytes < nbytes)
				stats.maxNodeBytes = nbytes;

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
