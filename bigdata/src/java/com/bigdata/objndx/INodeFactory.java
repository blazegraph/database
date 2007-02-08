package com.bigdata.objndx;

/**
 * Interface for creating nodes or leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface INodeFactory {

    /**
     * Create a node. The implementation is encouraged to steal the <i>keys</i>
     * and <i>childAddr</i> references rather than cloning them.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address from which the node was read.
     * @param branchingFactor
     *            The branching factor for the node.
     * @param nentries
     *            The #of entries spanned by this node.
     * @param keys
     *            A representation of the defined keys in the node.
     * @param childAddr
     *            An array of the persistent addresses for the children of this
     *            node.
     * @param childEntryCount
     *            An of the #of entries spanned by each direct child.
     * 
     * @return A node initialized from those data.
     */
    public INodeData allocNode(IIndex btree, long addr, int branchingFactor,
            int nentries, IKeyBuffer keys, long[] childAddr,
            int[] childEntryCount);

    /**
     * Create a leaf. The implementation is encouraged to steal the <i>keys</i>
     * and <i>values</i> references rather than cloning them.
     * 
     * @param btree
     *            The owning btree.
     * @param addr
     *            The address from which the leaf was read.
     * @param branchingFactor
     *            The branching factor for the leaf.
     * @param keys
     *            A representation of the defined keys in the node.
     * @param values
     *            An array containing the values found in the leaf.
     * 
     * @return A leaf initialized from those data.
     */
    public ILeafData allocLeaf(IIndex btree, long addr, int branchingFactor,
            IKeyBuffer keys, Object[] values);

}
