package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;

import com.bigdata.bop.CoreBaseBOp;

import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * AST node base class.
 */
public abstract class QueryNodeBase implements IQueryNode {

	public QueryNodeBase() {
	}
	
    @Override
    public final String toString() {

        return toString(0/* indent */);
        
    }

    /**
     * Visits itself.
     */
    @SuppressWarnings("unchecked")
    public Iterator<IQueryNode> preOrderIterator() {

        return new Striterator(new SingleValueIterator(this));
        
    }

    /**
     * Returns a string that may be used to indent a dump of the nodes in the
     * tree.
     * 
     * @param depth
     *            The indentation depth.
     * 
     * @return A string suitable for indent at that height.
     * 
     * FIXME Use {@link CoreBaseBOp#indent(int)}
     */
    protected static String indent(final int depth) {

        if (depth < 0) {

            return "";

        }

        return ws.substring(0, depth *2);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

}
