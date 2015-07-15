package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataBNodeImpl;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.striterator.AbstractChunkedResolverator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link ISPO}s to RDF
 * {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataStatementIteratorImpl
        extends
        AbstractChunkedResolverator<ISPO, BigdataStatement, AbstractTripleStore>
        implements BigdataStatementIterator {

    final private static Logger log = Logger
            .getLogger(BigdataStatementIteratorImpl.class);

    /**
     * An optional map of known blank node term identifiers and the
     * corresponding {@link BigdataBNodeImpl} objects. This map may be used to
     * resolve term identifiers to the corresponding blank node objects across a
     * "connection" context.
     */
    private final Map<IV, BigdataBNode> bnodes;

    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     */
    public BigdataStatementIteratorImpl(final AbstractTripleStore db,
            final IChunkedOrderedIterator<ISPO> src) {

        this(db, null/* bnodes */, src);
        
    }

    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param bnodes
     *            An optional map of known blank node term identifiers and the
     *            corresponding {@link BigdataBNodeImpl} objects. This map may
     *            be used to resolve blank node term identifiers to blank node
     *            objects across a "connection" context.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     */
    public BigdataStatementIteratorImpl(final AbstractTripleStore db,
            final Map<IV, BigdataBNode> bnodes,
                final IChunkedOrderedIterator<ISPO> src) {
        
        super(db, src, new BlockingBuffer<BigdataStatement[]>(
                db.getChunkOfChunksCapacity(), 
                db.getChunkCapacity(),
                db.getChunkTimeout(),
                TimeUnit.MILLISECONDS));

        this.bnodes = bnodes;
        
    }

    /**
     * Strengthens the return type.
     */
    @Override
    public BigdataStatementIteratorImpl start(final ExecutorService service) {

        return (BigdataStatementIteratorImpl) super.start(service);
        
    }

    /**
     * Resolve a chunk of {@link ISPO}s into a chunk of {@link BigdataStatement}s.
     */
    @Override
    protected BigdataStatement[] resolveChunk(final ISPO[] chunk) {

        if (log.isDebugEnabled())
            log.debug("chunkSize=" + chunk.length);

        /*
         * Create a collection of the distinct term identifiers used in this
         * chunk.
         */
        
        final Collection<IV<?, ?>> ivs = new LinkedHashSet<IV<?, ?>>(
                chunk.length * state.getSPOKeyArity());

        for (ISPO spo : chunk) {

            {
                
                final IV<?,?> s = spo.s();

//                if (bnodes == null || !bnodes.containsKey(s))
//                    ivs.add(s);
                
                handleIV(s, ivs);
            
            }

//            ivs.add(spo.p());

            handleIV(spo.p(), ivs);
            
            {

                final IV<?,?> o = spo.o();

//                if (bnodes == null || !bnodes.containsKey(o))
//                    ivs.add(o);

                handleIV(o, ivs);
                
            }

            {
             
                final IV<?,?> c = spo.c();

//                if (c != null
//                        && (bnodes == null || !bnodes.containsKey(c)))
//                    ivs.add(c);

                if (c != null) 
                	handleIV(c, ivs);
                
            }

        }

        if (log.isDebugEnabled())
            log.debug("Resolving " + ivs.size() + " term identifiers");
        
        /*
         * Batch resolve term identifiers to BigdataValues, obtaining the
         * map that will be used to resolve term identifiers to terms for
         * this chunk.
         */
        final Map<IV<?, ?>, BigdataValue> terms = state.getLexiconRelation()
                .getTerms(ivs);

        final BigdataValueFactory valueFactory = state.getValueFactory();
        
        /*
         * The chunk of resolved statements.
         */
        final BigdataStatement[] stmts = new BigdataStatement[chunk.length];
        
        int i = 0;
        for (ISPO spo : chunk) {

            /*
             * Resolve term identifiers to terms using the map populated when we
             * fetched the current chunk.
             */
            final BigdataResource s = (BigdataResource) resolve(terms, spo.s());
            final BigdataURI p = (BigdataURI) resolve(terms, spo.p());
//            try {
//                p = (BigdataURI) resolve(terms, spo.p());
//            } catch (ClassCastException ex) {
//                log.error("spo="+spo+", p="+resolve(terms, spo.p()));
//                throw ex;
//            }
            final BigdataValue o = resolve(terms, spo.o());
            final IV<?,?> _c = spo.c();
            final BigdataResource c;
            if (_c != null) {
                /*
                 * FIXME This kludge to strip off the null graph should be
                 * isolated to the BigdataSail's package. Our own code should be
                 * protected from this behavior. Also see the
                 * BigdataSolutionResolverator.
                 */
                final BigdataResource tmp = (BigdataResource) resolve(terms, _c);
                if (tmp instanceof BigdataURI
                        && ((BigdataURI) tmp).equals(BD.NULL_GRAPH)) {
                    /*
                     * Strip off the "nullGraph" context.
                     */
                    c = null;
                } else {
                    c = tmp;
                }
            } else {
                c = null;
            }

            if (spo.hasStatementType() == false) {

                log.error("statement with no type: "
                        + valueFactory.createStatement(s, p, o, c, null, spo.getUserFlag()));

            }

            // the statement.
            final BigdataStatement stmt = valueFactory.createStatement(s, p, o,
                    c, spo.getStatementType(), spo.getUserFlag());

            // save the reference.
            stmts[i++] = stmt;

        }

        return stmts;

    }

    /**
     * Add the IV to the list of terms to materialize, and also
     * delegate to {@link #handleSid(SidIV, Collection, boolean)} if it's a
     * SidIV.
     */
    private void handleIV(final IV<?, ?> iv, 
    		final Collection<IV<?, ?>> ids) {
    	
//    	if (iv instanceof SidIV) {
//    		
//    		handleSid((SidIV<?>) iv, ids);
//    		
//    	}
    		
    	if (bnodes == null || !bnodes.containsKey(iv)) {
    	
    		ids.add(iv);
    		
    	}
    	
    }
    
//    /**
//     * Sids need to be handled specially because their individual ISPO
//     * components might need materialization as well.
//     */
//    private void handleSid(final SidIV<?> sid,
//    		final Collection<IV<?, ?>> ids) {
//    	
//    	final ISPO spo = sid.getInlineValue();
//    	
//    	handleIV(spo.s(), ids);
//    	
//    	handleIV(spo.p(), ids);
//    	
//    	handleIV(spo.o(), ids);
//    	
//    	if (spo.c() != null) {
//    		
//        	handleIV(spo.c(), ids);
//    		
//    	}
//
//    }


    
    /**
     * Resolve a term identifier to the {@link BigdataValue}, checking the
     * {@link #bnodes} map if it is defined.
     * 
     * @param terms
     *            The terms mapping obtained from the lexicon.
     * @param iv
     *            The term identifier.
     *            
     * @return The {@link BigdataValue}.
     */
    private BigdataValue resolve(final Map<IV<?,?>, BigdataValue> terms,
            final IV<?,?> iv) {

        BigdataValue v = null;

        if (bnodes != null) {

            v = bnodes.get(iv);

        }

        if (v == null) {

            v = terms.get(iv);

        }

        return v;

    }

}
