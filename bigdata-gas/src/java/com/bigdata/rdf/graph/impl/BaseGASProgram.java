/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.Factory;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASState;

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
abstract public class BaseGASProgram<VS, ES, ST> implements
        IGASProgram<VS, ES, ST> {

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation does not restrict the visitation to a
     * connectivity matrix (returns <code>null</code>).
     */
    @Override
    public URI getLinkType() {
        
        return null;
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns its argument.
     */
    @Override
    public IStriterator constrainFilter(final IGASContext<VS, ES, ST> ctx,
            final IStriterator itr) {

        return itr;
        
    }
    
    /**
     * Return an {@link IFilter} that will only visit the edges of the graph.
     * 
     * @see IGASState#isEdge(Statement)
     */
    protected IFilter getEdgeOnlyFilter(final IGASContext<VS, ES, ST> ctx) {

        return new EdgeOnlyFilter(ctx);
        
    }
    
    /**
     * Filter visits only edges (filters out attribute values).
     * <p>
     * Note: This filter is pushed down onto the AP and evaluated close to the
     * data.
     */
    private class EdgeOnlyFilter extends Filter {
        private static final long serialVersionUID = 1L;
        private final IGASState<VS, ES, ST> gasState;
        private EdgeOnlyFilter(final IGASContext<VS, ES, ST> ctx) {
            this.gasState = ctx.getGASState();
        }
        @Override
        public boolean isValid(final Object e) {
            return gasState.isEdge((Statement) e);
        }
    };
    
    /**
     * Return a filter that only visits the edges of graph that are instances of
     * the specified link attribute type.
     * <p>
     * Note: For bigdata, the visited edges can be decoded to recover the
     * original link as well. 
     * 
     * @see IGASState#isLinkAttrib(Statement, URI)
     * @see IGASState#decodeStatement(Value)
     */
    protected IFilter getLinkAttribFilter(final IGASContext<VS, ES, ST> ctx,
            final URI linkAttribType) {

        return new LinkAttribFilter(ctx, linkAttribType);

    }

    /**
     * Filter visits only edges where the {@link Statement} is an instance of
     * the specified link attribute type. For bigdata, the visited edges can be
     * decoded to recover the original link as well.
     */
    private class LinkAttribFilter extends Filter {
        private static final long serialVersionUID = 1L;

        private final IGASState<VS, ES, ST> gasState;
        private final URI linkAttribType;
        
        public LinkAttribFilter(final IGASContext<VS, ES, ST> ctx,
                final URI linkAttribType) {
            if (linkAttribType == null)
                throw new IllegalArgumentException();
            this.gasState = ctx.getGASState();
            this.linkAttribType = linkAttribType;
        }

        @Override
        public boolean isValid(final Object e) {
            return gasState.isLinkAttrib((Statement) e, linkAttribType);
        }
    }
    
//    /**
//     * If the vertex is actually an edge, then return the decoded edge.
//     * 
//     * @see GASUtil#decodeStatement(Value)
//     */
//    protected Statement decodeStatement(final Value v) {
//       
//        return GASUtil.decodeStatement(v);
//        
//    }

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
    public void init(final IGASState<VS, ES, ST> state, final Value u) {

        // NOP

    }

//    public Factory<Value, VS> getVertexStateFactory();

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>null</code>. Override this if
     * the algorithm uses per-edge computation state.
     */
    @Override
    public Factory<Statement, ES> getEdgeStateFactory() {

        return null;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation returns <code>true</code>. Override this if
     * you know whether or not the computation state of this vertex has changed.
     */
    @Override
    public boolean isChanged(IGASState<VS, ES, ST> state, Value u) {

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
