package com.bigdata.htree;

import com.bigdata.btree.NodeSerializer;
import com.bigdata.htree.data.IBucketData;
import com.bigdata.htree.data.IDirectoryData;

/**
 * Factory for mutable nodes and leaves used by the {@link NodeSerializer}.
 */
class NodeFactory implements INodeFactory {

	public static final INodeFactory INSTANCE = new NodeFactory();

	private NodeFactory() {
	}

	public BucketPage allocLeaf(final AbstractHTree btree, final long addr,
			final IBucketData data) {

		return new BucketPage((HTree) btree, addr, data);

	}

	public DirectoryPage allocNode(final AbstractHTree btree, final long addr,
			final IDirectoryData data) {

		return new DirectoryPage((HTree) btree, addr, data);

	}

}
