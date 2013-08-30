package com.bigdata.rdf.graph.impl.bd;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ajax.JSON.Literal;
import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.GASState;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine.BigdataGraphAccessor;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;

public class BigdataGASState<VS, ES, ST> extends GASState<VS, ES, ST> {

    static private final Logger log = Logger.getLogger(BigdataGASState.class);

    @Override
    protected BigdataGraphAccessor getGraphAccessor() {

        return (BigdataGraphAccessor) super.getGraphAccessor();

    }

    public BigdataGASState(final BigdataGraphAccessor graphAccessor,
            final IStaticFrontier frontier,
            final IGASSchedulerImpl gasScheduler,
            final IGASProgram<VS, ES, ST> gasProgram) {

        super(graphAccessor, frontier, gasScheduler, gasProgram);

    }

    /**
     * {@inheritDoc}
     * 
     * TODO EDGE STATE: edge state should be traced out also.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void traceState() {

        super.traceState();

        if (!log.isTraceEnabled())
            return;

        final AbstractTripleStore kb = getGraphAccessor().getKB();

        // Get all terms in the frontier.
        final Set<IV<?, ?>> tmp = new HashSet<IV<?, ?>>((Collection) frontier());

        // Add all IVs for the vertexState.
        tmp.addAll((Collection) vertexState.keySet());

        // Batch resolve all IVs.
        final Map<IV<?, ?>, BigdataValue> m = kb.getLexiconRelation().getTerms(
                tmp);

        log.trace("frontier: size=" + frontier().size());

        for (Value v : frontier()) {

            log.trace("frontier: iv=" + v + " (" + m.get(v) + ")");

        }

        log.trace("vertexState: size=" + vertexState.size());

        for (Map.Entry<Value, VS> e : vertexState.entrySet()) {

            final Value v = e.getKey();

            final BigdataValue val = m.get(v);

            log.trace("vertexState: vertex=" + v + " (" + val + "), state="
                    + e.getValue());

        }

    }

    @Override
    public String toString(final Statement e) {

        return getGraphAccessor().getKB().toString((ISPO) e);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: The {@link IV} classes sometimes implement more than one kind of
     * {@link Value}. E.g., {@link TermId} can be a {@link BNode}, {@link URI},
     * or {@link Literal} and implements ALL of those interfaces. So we have to
     * make an {@link IV}-specific check here.
     * 
     * TODO This is visiting all edges, including link attributes (aka
     * hyperedges or statements about statements). Should we further restrict
     * traversal to only simple edges (by checking that the source and target
     * vertices are not {@link SidIV}s).
     */
    @Override
    public boolean isEdge(final Statement e) {

        final ISPO spo = (ISPO) e;
        
        return spo.o().isURI(); // FIXME CORRECTNESS : isResource()
        
    }
   
    @Override
    public boolean isAttrib(final Statement e) {
        return !isEdge(e);
    }
   
    @Override
    public boolean isLinkAttrib(final Statement e,
            final URI linkAttribType) {
        final ISPO edge = (ISPO) e;
        if (!edge.p().equals(linkAttribType)) {
            // Edge does not use the specified link attribute type.
            return false;
        }
        if (!(edge.s() instanceof SidIV)) {
            // The subject of the edge is not a Statement.
            return false;
        }
        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Statement decodeStatement(final Value v) {

        if (!(v instanceof IV))
            return null;

        final IV tmp = (IV) v;

        if (!tmp.isStatement())
            return null;

        final ISPO decodedEdge = (ISPO) tmp.getInlineValue();

        return decodedEdge;

    }

}
