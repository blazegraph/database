package com.bigdata.htree;

import com.bigdata.btree.NodeSerializer;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.data.INodeData;

/**
     * Factory for mutable nodes and leaves used by the {@link NodeSerializer}.
     */
    class NodeFactory implements INodeFactory {

        public static final INodeFactory INSTANCE = new NodeFactory();

        private NodeFactory() {
        }

        public BucketPage allocLeaf(final AbstractHTree btree, final long addr,
                final ILeafData data) {

// FIXME           return new BucketPage(btree, addr, data);
        	throw new UnsupportedOperationException();

        }

        public DirectoryPage allocNode(final AbstractHTree btree, final long addr,
                final INodeData data) {

// FIXME            return new DirectoryPage(btree, addr, data);
        	throw new UnsupportedOperationException();

        }

    }