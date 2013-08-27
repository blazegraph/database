package com.bigdata.rdf.graph.impl;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.spo.ISPO;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.IFilter;
import cutthecrap.utils.striterators.IStriterator;

/**
 * Abstract base class with some useful defaults.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <VS>
 * @param <ES>
 * @param <ST>
 */
@SuppressWarnings("rawtypes")
abstract public class BaseGASProgram<VS, ES, ST> implements
        IGASProgram<VS, ES, ST> {

    /**
     * Filter visits only edges (filters out attribute values).
     * <p>
     * Note: This filter is pushed down onto the AP and evaluated close to the
     * data.
     */
    protected static final IFilter edgeOnlyFilter = new Filter() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isValid(final Object e) {
            return ((ISPO) e).o().isURI();
        }
    };

    /**
     * Return <code>true</code> iff the visited {@link ISPO} is an instance
     * of the specified link attribute type.
     * 
     * @return
     */
    protected static final IFilter newLinkAttribFilter(final IV linkAttribType) {

        return new LinkAttribFilter(linkAttribType);
        
    }

    static class LinkAttribFilter extends Filter {

        private static final long serialVersionUID = 1L;

        private final IV linkAttribType;
        
        public LinkAttribFilter(final IV linkAttribType) {
         
            if (linkAttribType == null)
                throw new IllegalArgumentException();

            this.linkAttribType = linkAttribType;
            
        }

        @Override
        public boolean isValid(final Object e) {
            final ISPO edge = (ISPO) e;
            if(!edge.p().equals(linkAttribType)) {
                // Edge does not use the specified link attribute type.
                return false;
            }
            if (!(edge.s() instanceof SidIV)) {
                // The subject of the edge is not a Statement.
                return false;
            }
            return true;
        }

    }
    
    /**
     * If the vertex is actually an edge, then return the decoded edge.
     * <p>
     * Note: A vertex may be an edge. A link attribute is modeled by treating
     * the link as a vertex and then asserting a property value about that
     * "link vertex". For bigdata, this is handled efficiently as inline
     * statements about statements. This approach subsumes the property graph
     * model (property graphs do not permit recursive nesting of these
     * relationships) and is 100% consistent with RDF reification, except that
     * the link attributes are modeled efficiently inline with the links. This
     * is what we call <a
     * href="http://www.bigdata.com/whitepapers/reifSPARQL.pdf" > Reification
     * Done Right </a>.
     * 
     * @param v
     *            The vertex.
     * 
     * @return The edge decoded from that vertex and <code>null</code> iff the
     *         vertex is not an edge.
     * 
     *         TODO RDR : Link to an RDR wiki page as well.
     * 
     *         TODO We can almost write the same logic at the openrdf layer
     *         using <code>v instanceof Statement</code>. However, v can not be
     *         a Statement for openrdf and there is no way to decode the vertex
     *         as a Statement in openrdf.
     */
    protected ISPO decodeStatement(final IV v) {

        if (!v.isStatement())
            return null;

        final ISPO decodedEdge = (ISPO) v.getInlineValue();

        return decodedEdge;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation does not restrict the visitation to a
     * connectivity matrix (returns <code>null</code>).
     */
    @Override
    public IV getLinkType() {
        
        return null;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns its argument.
     */
    @Override
    public IStriterator constrainFilter(IStriterator itr) {
        
        return itr;
        
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The default gathers on the {@link EdgesEnum#InEdges}.
     */
    @Override
    public EdgesEnum getGatherEdges() {

        return EdgesEnum.InEdges;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default scatters on the {@link EdgesEnum#OutEdges}.
     */
    @Override
    public EdgesEnum getScatterEdges() {

        return EdgesEnum.OutEdges;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default is a NOP.
     */
    @Override
    public void init(final IGASState<VS, ES, ST> state, final IV u) {

        // NOP

    }

//    public Factory<IV, VS> getVertexStateFactory();

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>null</code>. Override this if
     * the algorithm uses per-edge computation state.
     */
    @Override
    public Factory<ISPO, ES> getEdgeStateFactory() {

        return null;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>true</code>. Override this if
     * you know whether or not the computation state of this vertex has changed.
     */
    @Override
    public boolean isChanged(IGASState<VS, ES, ST> state, IV u) {

        return true;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default returns <code>true</code>.
     */
    @Override
    public boolean nextRound(IGASContext<VS, ES, ST> ctx) {

        return true;

    }

}
