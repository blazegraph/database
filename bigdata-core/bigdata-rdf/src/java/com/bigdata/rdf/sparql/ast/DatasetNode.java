package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.Dataset;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.eval.DataSetSummary;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * AST node models a SPARQL default graph and/or named graph data set.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class DatasetNode extends QueryNodeBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends QueryNodeBase.Annotations {
        
        /**
         * A {@link DataSetSummary} for the default graph.
         */
        String DEFAULT_GRAPHS = "defaultGraphs";

        /**
         * An optional filter for the default graph.
         */
        String DEFAULT_GRAPH_FILTER = "defaultGraphFilter";

        /** A {@link DataSetSummary} for the named graphs. */
        String NAMED_GRAPHS = "namedGraphs";

        /**
         * An optional filter for the named graphs.
         */
        String NAMED_GRAPH_FILTER = "namedGraphFilter";
        
    }
	
    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public DatasetNode(final DatasetNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public DatasetNode(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * 
     * @param ivs
     *            The {@link IV}s.
     * @param update
     *            <code>true</code> iff this is a SPARQL UPDATE./
     * @return
     */
    @SuppressWarnings("rawtypes")
    private static final DataSetSummary asDataSetSummary(final Set<IV> ivs,
            final boolean update) {
        /*
         * Note: This will cause named-graphs-01b to fail.
         */
        // if (ivs == null)
        // return null;

        /*
         * Note: Per DAWG tests graph-02 and graph-04, a query against an empty
         * default graph collection or an empty named graph collection should
         * be constrained to NO graphs.  This is different from the case where
         * the dataset is simply not specified, which is interpreted as having
         * no constraint on the visited graphs.  
         * 
         * See DataSetSummary#toInternalValues()
         */
//        if (ivs.isEmpty())
//            return null;
        
        return new DataSetSummary((Set) (ivs == null ? Collections.emptySet()
                : ivs), update);
        
    }

    /**
     * 
     * @param defaultGraphs
     * @param namedGraphs
     * @param update
     *            <code>true</code> iff this is a SPARQL update.
     */
    @SuppressWarnings("rawtypes")
    public DatasetNode(final Set<IV> defaultGraphs, final Set<IV> namedGraphs,
            final boolean update) {

        this(asDataSetSummary(defaultGraphs, update), asDataSetSummary(
                namedGraphs, update));

	}
	
	@SuppressWarnings("rawtypes")
    public DatasetNode(final Set<IV> defaultGraphs, final Set<IV> namedGraphs,
	        final IElementFilter<ISPO> defaultGraphFilter, 
            final IElementFilter<ISPO> namedGraphFilter,
            final boolean update) {
        
        this(asDataSetSummary(defaultGraphs, update), asDataSetSummary(
                namedGraphs, update), defaultGraphFilter, namedGraphFilter);
        
    }
	
    public DatasetNode(final DataSetSummary defaultGraphs,
            final DataSetSummary namedGraphs) {

        this(defaultGraphs, namedGraphs, null, null);

    }
	
	public DatasetNode(final Dataset dataset, final boolean update) {
		
        this(DataSetSummary.toInternalValues(dataset.getDefaultGraphs()),
                DataSetSummary.toInternalValues(dataset.getNamedGraphs()),
                update);

	}
	
    /**
     * Core constructor implementation.
     *  
     * @param defaultGraphs
     *            The list of default graphs (optional). When not specified, all
     *            graphs will be used unless a <i>defaultGraphsFilter</i> is
     *            applied.
     * @param namedGraphs
     *            The list of named graphs (optional). When not specified, all
     *            graphs will be used unless a <i>namedGraphsFilter</i> is
     *            applied.
     * @param defaultGraphFilter
     *            A filter for default graphs (optional and typically only used
     *            when the <i>defaultGraphs</i> is <code>null</code>, e.g., to
     *            apply ACLs).
     * @param namedGraphFilter
     *            A filter for named graphs (optional and typically only used
     *            when the <i>namedGraphs</i> is <code>null</code>, e.g., to
     *            apply ACLs).
     */
	public DatasetNode(
	        final DataSetSummary defaultGraphs, 
	        final DataSetSummary namedGraphs,
			final IElementFilter<ISPO> defaultGraphFilter, 
			final IElementFilter<ISPO> namedGraphFilter) {
		
		setDefaultGraphs(defaultGraphs);

		setNamedGraphs(namedGraphs);
		
        setDefaultGraphFilter(defaultGraphFilter);
        
        setNamedGraphFilter(namedGraphFilter);
        
	}

    public void setDefaultGraphs(final DataSetSummary defaultGraphs) {

        setProperty(Annotations.DEFAULT_GRAPHS, defaultGraphs);

    }

    public void setNamedGraphs(final DataSetSummary namedGraphs) {

        setProperty(Annotations.NAMED_GRAPHS, namedGraphs);

    }

    public void setDefaultGraphFilter(
            final IElementFilter<ISPO> defaultGraphFilter) {

        setProperty(Annotations.DEFAULT_GRAPH_FILTER, defaultGraphFilter);

    }

    public void setNamedGraphFilter(final IElementFilter<ISPO> namedGraphFilter) {

        setProperty(Annotations.NAMED_GRAPH_FILTER, namedGraphFilter);

    }

	public DataSetSummary getDefaultGraphs() {
        
	    return (DataSetSummary) getProperty(Annotations.DEFAULT_GRAPHS);
	    
	}
	
	public DataSetSummary getNamedGraphs() {
		
	    return (DataSetSummary) getProperty(Annotations.NAMED_GRAPHS);
	    
	}

    @SuppressWarnings("unchecked")
    public IElementFilter<ISPO> getDefaultGraphFilter() {
        
        return (IElementFilter<ISPO>) getProperty(Annotations.DEFAULT_GRAPH_FILTER);
        
    }

    @SuppressWarnings("unchecked")
    public IElementFilter<ISPO> getNamedGraphFilter() {

        return (IElementFilter<ISPO>) getProperty(Annotations.NAMED_GRAPH_FILTER);
        
    }

    @Override
    public String toString(final int indent) {
        final String s = indent(indent);
        final StringBuilder sb = new StringBuilder();
        final DataSetSummary defaultGraphs = getDefaultGraphs();
        final DataSetSummary namedGraphs = getNamedGraphs();
        final IElementFilter<?> defaultGraphFilter = getDefaultGraphFilter();
        final IElementFilter<?> namedGraphFilter = getNamedGraphFilter();
        if (defaultGraphs != null) {
            sb.append("\n");
            sb.append(s);
            sb.append("defaultGraphs=");
            sb.append(defaultGraphs.toString());
        }
        if (namedGraphs != null) {
            sb.append("\n");
            sb.append(s);
            sb.append("namedGraphs=");
            sb.append(namedGraphs.toString());
        }
        if (defaultGraphFilter != null) {
            sb.append("\n");
            sb.append(s);
            sb.append("defaultGraphFilter=" + defaultGraphFilter);
        }
        if (namedGraphFilter != null) {
            sb.append("\n");
            sb.append(s);
            sb.append("namedGraphFilter=" + namedGraphFilter);
        }
//        if (getQueryHints() != null) {
//            sb.append("\n");
//            sb.append(s);
//            sb.append(Annotations.QUERY_HINTS);
//            sb.append("=");
//            sb.append(getQueryHints().toString());
//        }
        return sb.toString();
    }

}
