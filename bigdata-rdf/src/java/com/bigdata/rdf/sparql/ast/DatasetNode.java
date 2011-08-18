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
 * 
 * TODO Why not an {@link IQueryNode}?
 */
public class DatasetNode {

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

}
