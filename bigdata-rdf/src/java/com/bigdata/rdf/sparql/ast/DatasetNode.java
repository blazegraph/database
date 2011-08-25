package com.bigdata.rdf.sparql.ast;

import java.util.Set;

import org.openrdf.query.Dataset;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sail.DataSetSummary;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.accesspath.IElementFilter;

/**
 * AST node models a SPARQL default graph and/or named graph data set.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class DatasetNode extends QueryNodeBase {

	private final DataSetSummary defaultGraphs, namedGraphs;
	
	private final IElementFilter<ISPO> defaultGraphFilter, namedGraphFilter;
	
	public DatasetNode(final Set<IV> defaultGraphs, final Set<IV> namedGraphs) {
		
		this(new DataSetSummary(defaultGraphs), new DataSetSummary(namedGraphs));
		
	}
	
	public DatasetNode(final Set<IV> defaultGraphs, final Set<IV> namedGraphs,
	        final IElementFilter<ISPO> defaultGraphFilter, 
            final IElementFilter<ISPO> namedGraphFilter) {
        
        this(defaultGraphs != null ? new DataSetSummary(defaultGraphs) : null,
                namedGraphs != null ? new DataSetSummary(namedGraphs) : null,
                defaultGraphFilter, namedGraphFilter);
        
    }
	
    public DatasetNode(final DataSetSummary defaultGraphs,
            final DataSetSummary namedGraphs) {

        this.defaultGraphs = defaultGraphs;
        this.namedGraphs = namedGraphs;

        this.defaultGraphFilter = null;
        this.namedGraphFilter = null;

    }
	
	public DatasetNode(final Dataset dataset) {
		
		this(DataSetSummary.toInternalValues(dataset.getDefaultGraphs()),
				DataSetSummary.toInternalValues(dataset.getNamedGraphs()));
		
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
		
		this.defaultGraphFilter = defaultGraphFilter;
		this.namedGraphFilter = namedGraphFilter;
		
		this.defaultGraphs = defaultGraphs;
		this.namedGraphs = namedGraphs;
		
	}
	
	public IElementFilter<ISPO> getDefaultGraphFilter() {
		return defaultGraphFilter;
	}
	
	public IElementFilter<ISPO> getNamedGraphFilter() {
		return namedGraphFilter;
	}
	
	public DataSetSummary getDefaultGraphs() {
		return defaultGraphs;
	}
	
	public DataSetSummary getNamedGraphs() {
		return namedGraphs;
	}

    @Override
    public String toString(final int indent) {
        final String s = indent(indent);
        final StringBuilder sb = new StringBuilder();
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
        return sb.toString();
    }

    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DatasetNode))
            return false;
        final DatasetNode t = (DatasetNode) o;
        if (defaultGraphs == null) {
            if (t.defaultGraphs != null)
                return false;
        } else if (!defaultGraphs.equals(t.defaultGraphs)) {
            return false;
        }
        if (namedGraphs == null) {
            if (t.namedGraphs != null)
                return false;
        } else if (!namedGraphs.equals(t.namedGraphs)) {
            return false;
        }
        if (defaultGraphFilter == null) {
            if (t.defaultGraphFilter != null)
                return false;
        } else if (!defaultGraphFilter.equals(t.defaultGraphFilter)) {
            return false;
        }
        if (namedGraphFilter == null) {
            if (t.namedGraphFilter != null)
                return false;
        } else if (!namedGraphFilter.equals(t.namedGraphFilter)) {
            return false;
        }
        return true;

    }
    
}
