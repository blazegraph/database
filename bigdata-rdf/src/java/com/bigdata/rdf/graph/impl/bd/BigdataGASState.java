package com.bigdata.rdf.graph.impl.bd;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.impl.GASState;
import com.bigdata.rdf.graph.impl.bd.BigdataGASEngine.BigdataGraphAccessor;
import com.bigdata.rdf.internal.IV;
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
            final IGASSchedulerImpl gasScheduler,
            final IGASProgram<VS, ES, ST> gasProgram) {

        super(graphAccessor, gasScheduler, gasProgram);

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
        final Set<IV<?, ?>> tmp = new HashSet<IV<?,?>>((Collection) frontier());

        // Add all IVs for the vertexState.
        tmp.addAll((Collection) vertexState.keySet());

        // Batch resolve all IVs.
        final Map<IV<?, ?>, BigdataValue> m = kb.getLexiconRelation().getTerms(
                tmp);

        log.trace("frontier: size=" + frontier().size());

        for (IV v : frontier()) {

            log.trace("frontier: iv=" + v + " (" + m.get(v) + ")");

        }

        log.trace("vertexState: size=" + vertexState.size());

        for (Map.Entry<IV, VS> e : vertexState.entrySet()) {

            final IV v = e.getKey();

            final BigdataValue val = m.get(v);

            log.trace("vertexState: vertex=" + v + " (" + val + "), state="
                    + e.getValue());

        }

    }

    @Override
    public String toString(final ISPO e) {

        return getGraphAccessor().getKB().toString(e);
        
    }

}
