package com.bigdata.objndx;

/**
 * A plan for building a B+-Tree based on an input branching factor and #of
 * entries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexSegmentPlan {

    /**
     * The branching factor of the output tree (input).
     */
    final int m;
    
    /**
     * The minimum #of values that may be placed into non-root leaf (and
     * also the minimum #of children that may be placed into a non-root
     * node). (the minimum capacity).
     */
    final int m2; 
    
    /**
     * The #of entries in the btree (input).
     */
    final int nentries;
    
    /**
     * The #of leaves that will exist in the output tree. When nleaves == 1
     * the output tree will consist of a root leaf. In this case we do not
     * open a temporary file for the nodes since there will not be any.
     */
    final int nleaves; 
    
    /**
     * The height of the output tree (#of levels in the output tree).
     */
    final int height;

    /**
     * The #of entries to place into each leaf.  The array is dimensioned to
     * {@link #nleaves}.
     */
    final int[] numInLeaf;
    
    /**
     * The #of nodes at each level of the tree above the leaves and [null]
     * iff there are no nodes in the output tree.
     * 
     * @see #nleaves, which is the #of leaves in the output tree.
     */
    final int[] numInLevel;
    
    /**
     * The #of children to place into each node in each level of the output
     * tree or [null] iff there are no nodes in the output tree. The first
     * index is the level in the tree, start from level zero which is the
     * root and increasing through level [height], which is the level above
     * the leaves.
     */
    final int[][] numInNode;
    
    /**
     * Create a plan for building a B+-Tree. The plan has only these two
     * inputs. Everything else about the plan is deterministic based on
     * those values.
     * 
     * @param m
     *            The branching factor of the output tree (#of keys/values
     *            for a leaf or the #of children for a node).
     * @param nentries
     *            The #of entries in the tree.
     */
    public IndexSegmentPlan(int m,int nentries) {

        assert m >= BTree.MIN_BRANCHING_FACTOR;
        assert nentries > 0;

        // The branching factor of the output tree.
        this.m = m;
        
        // The #of entries in the btree.
        this.nentries = nentries;
        
        // The minimum capacity of a leaf (or a node).
        m2 = (m+1)/2; 
        
        // The #of leaves in the output tree.
        nleaves = (int)Math.ceil((double)nentries / (double)m); 
        
        // The height of the output tree.
        height = getMinimumHeight(m,nleaves);

        IndexSegmentBuilder.log.info("branchingFactor="+m+", nentries="+nentries+", nleaves="+nleaves+", height="+height);
        
        // #of entries in each leaf.
        numInLeaf = distributeKeys(m,m2,nleaves,nentries);
        
        if (height == 0) {
            
            /*
             * There are no nodes in the output tree.
             */
            
            numInNode = null;
            
            numInLevel = null;
            
        } else {
            
            /*
             * Figure out how many nodes are in each level of the output tree.
             * We start from the leaves and compute the #of nodes required to
             * hold that many child references.
             */
            
            numInNode = new int[height][];
            
            numInLevel = new int[height];

            /*
             * The first time through this loop the #of children is initialized
             * to the #of leaves. Thereafter is is [numThisLevel] for the
             * previous level.
             */
            int nchildren = nleaves;
            
            for (int h = height - 1; h >= 0; h--) {

                /*
                 * Compute the minimum #of nodes required to hold the references
                 * for the children of the level in the tree beneath this one.
                 */
                int numThisLevel = (int)Math.ceil((double)nchildren / (double)m); 
                
                numInLevel[h] = numThisLevel;
                
                /*
                 * Distribute the children among the nodes allocated for this level.
                 */
                numInNode[h] = distributeChildren(m, m2, numThisLevel, nchildren);
                
                nchildren = numThisLevel;
                
            }
            
        }

    }
    
    /**
     * Chooses the minimum height for a tree having a specified branching factor
     * and a specified #of leaves.
     * 
     * @param m
     *            The branching factor.
     * @param nleaves
     *            The #of leaves that must be addressable by the tree.
     */
    public static int getMinimumHeight(int m, int nleaves) {
        
        final int maxHeight = 10;
        
        for (int h = 0; h <= maxHeight; h++) {
        
            /*
             * The maximum #of leaves addressable by a tree of height h and the
             * given branching factor.
             * 
             * Note: Java guarentees that Math.pow(int,int) produces the exact
             * result iff that result can be represented as an integer. This
             * useful feature lets us avoid having to deal with precision issues
             * or write our own integer version of pow (computing m*m h times).
             */
            final double d = (double)Math.pow(m,h);
            
            if( d >= nleaves ) {
            
                /*
                 * h is the smallest height tree of the specified branching
                 * factor m capable of addressing the specified #of leaves.
                 */
                return h;
                
            }
            
        }
        
        throw new UnsupportedOperationException(
                "Can not build a tree for that many leaves: m=" + m
                        + ", nleaves=" + nleaves + ", maxHeight=" + maxHeight);
    }

    /**
     * Distributes the keys among the leaves.
     * 
     * We want to fill up every leaf, but we have to make sure that the last
     * leaf is not under capacity. To that end, we calculate the #of entries
     * that would remain if we filled up n-1 leaves completely. If the #of
     * remaining entries is less than or equal to the minimum capacity of a
     * leaf, then we have to adjust the allocation of entries such that the last
     * leaf is at its minimum capacity. This is done by computing the shortage
     * and then distributing that shortage among the leaves. Once we have
     * deferred enough entries we are guarenteed that the final leaf will not be
     * under capacity.
     * 
     * @param m
     *            The branching factor in the output tree.
     * @param m2
     *            The minimum capacity for a leaf in the output tree, which is
     *            computed as (m+1)/2.
     * @param nleaves
     *            The #of leaves in the output tree.
     * @param nentries
     *            The #of entries to be inserted into the output tree.
     * 
     * @return An array indicating how many entries should be inserted into each
     *         leaf of the output tree. The array index is the leaf order
     *         (origin zero). The value is the capacity to which that leaf
     *         should be filled.
     * 
     * @see TestIndexSegmentPlan
     * @see TestIndexSegmentBuilderWithSmallTree#test_problem3_buildOrder3()
     */
    public static int[] distributeKeys(int m, int m2, int nleaves, int nentries) {

        assert m >= BTree.MIN_BRANCHING_FACTOR;
        assert m2 >= (m + 1) / 2;
        assert m2 <= m;
        assert nleaves > 0;
        assert nentries > 0;

        if (nleaves == 1) {
            
            /*
             * If there is just a root leaf then any number (up to the leafs
             * capacity) will fit into that root leaf.
             */
            
            assert nentries <= m;
            
            return new int[]{nentries};
            
        }
            
        final int[] n = new int[nleaves];

        /*
         * Default each leaf to m entries.
         */
        for (int i = 0; i < nleaves; i++) {
            
            n[i] = m;
            
        }

        /*
         * The #of entries that would be allocated to the last leaf if we filled
         * each proceeding leaf to capacity.
         */
        int remaining = nentries - ((nleaves - 1) * m);

        /*
         * If the #of entries remainin would put the leaf under capacity then we
         * compute the shortage. We need to defer this many entries from the
         * previous leaves in order to have the last leaf reach its minimum
         * capacity.
         */
        int shortage = remaining < m2 ? m2 - remaining : 0;

        if( remaining < m2 ) {

            // the last leaf will be at minimum capacity.
            n[nleaves - 1] = m2;
            
        } else {

            // the remainder will go into the last leaf without underflow.
            n[nleaves - 1] = remaining;
            
        }

        /*
         * If the shortage is greater than the #of previous leaves, then we need
         * to short some leaves by more than one entry. This scenario can be
         * observed when building a tree with m := 9 and 10 entries. In that
         * case there are only two leaves and we wind up shorting the previous
         * leaf by 4 bringing both leaves down to their minimum capacity of 5.
         */
        if (shortage > 0) {
            
            while (shortage > 0) {

                for (int i = nleaves - 2; i >= 0 && shortage > 0; i--) {

                    n[i]--;

                    shortage--;

                }
                
            }
            
        }

        return n;
        
    }
    
    /**
     * Distributes the children among the nodes of a given level.
     * 
     * Note: This is just an alias for
     * {@link #distributeKeys(int, int, int, int)}. The only difference when
     * distributing children among nodes is that the result returned to the
     * caller must be interpreted as the #of children to assigned to each node
     * NOT the #of keys (for leaves the #of values and the #of keys is always
     * the same).
     * 
     * @param m
     *            The branching factor in the output tree.
     * @param m2
     *            The minimum capacity, which should be computed as (m+1)/2.
     * @param nnodes
     *            The #of nodes in the output tree for some given level of the
     *            output tree.
     * @param nchildren
     *            The #of children to be distributed among those nodes.
     * 
     * @return An array indicating how many children should be inserted into
     *         each node of the output tree at the given level. The array index
     *         is the node order (origin zero). The value is the #of children
     *         which must be assigned to that leaf.
     * 
     * @see TestIndexSegmentPlan
     * @see TestIndexSegmentBuilderWithSmallTree#test_problem3_buildOrder3()
     */
    public static int[] distributeChildren(int m, int m2, int nnodes, int nchildren) {

        return distributeKeys(m,m2,nnodes,nchildren);
        
    }
    
}
