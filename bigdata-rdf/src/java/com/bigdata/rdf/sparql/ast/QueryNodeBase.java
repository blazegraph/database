package com.bigdata.rdf.sparql.ast;

/**
 * AST node base class.
 */
public abstract class QueryNodeBase implements IQueryNode {

	private IGroupNode parent;
	
	public QueryNodeBase() {
	}

	public void setParent(final IGroupNode parent) {
		this.parent = parent;
	}

	public IGroupNode getParent() {
		return parent;
	}
	
    /**
     * Returns a string that may be used to indent a dump of the nodes in the
     * tree.
     * 
     * @param depth
     *            The indentation depth.
     * 
     * @return A string suitable for indent at that height.
     */
    protected static String indent(final int depth) {

        if (depth < 0) {

            return "";

        }

        return ws.substring(0, depth *2);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

}
