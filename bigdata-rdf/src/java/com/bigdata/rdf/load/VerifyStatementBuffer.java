package com.bigdata.rdf.load;

import java.beans.Statement;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Statements inserted into the buffer are verified against the database. No
 * new {@link Value}s or {@link Statement}s will be written on the
 * database by this class. The #of {@link URI}, {@link Literal}, and told
 * triples not found in the database are reported by various counters.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo The counters are being updated on each incremental write rather
 * than tracked on a per-task basis and then updated iff the task as a whole
 * succeeds. This causes double-counting of both found and not found totals
 * when a task errors and then retries. The counters need to be attached to
 * the task and the task logic extended to capture them rather than to the
 * statement buffer (a bit of a mess).
 */
public class VerifyStatementBuffer extends StatementBuffer {

    final protected static Logger log = Logger.getLogger(VerifyStatementBuffer.class);
    
    /**
     * True iff the {@link #log} level is WARN or less.
     */
    final protected  static boolean WARN = log.getEffectiveLevel().toInt() <= Level.WARN
            .toInt();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected  static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    final AtomicLong nterms, ntermsNotFound, ntriples, ntriplesNotFound;
    
    /**
     * @param database
     * @param capacity
     */
    public VerifyStatementBuffer(AbstractTripleStore database,
            int capacity, AtomicLong nterms, AtomicLong ntermsNotFound,
            AtomicLong ntriples, AtomicLong ntriplesNotFound) {
        
        super(database, capacity);

        this.nterms = nterms;
        
        this.ntermsNotFound = ntermsNotFound;
        
        this.ntriples = ntriples;
        
        this.ntriplesNotFound = ntriplesNotFound;
        
    }
    
    /**
     * Overriden to batch verify the terms and statements in the buffer.
     * 
     * FIXME Verify that {@link StatementBuffer#flush()} is doing the right
     * thing for this case (esp, how it handles bnodes when appearing as
     * {s,p,o} or when appearing as the statement identifier).
     */
    protected void incrementalWrite() {

        if (INFO) {
            log.info("numValues=" + numValues + ", numStmts=" + numStmts);
        }

        // Verify terms (batch operation).
        if (numValues > 0) {

            database.getLexiconRelation()
                    .addTerms(values, numValues, true/* readOnly */);

        }

        for( int i=0; i<numValues; i++ ) {
            
            final BigdataValue v = values[i];

            nterms.incrementAndGet();

            if (v.getTermId() == IRawTripleStore.NULL) {
                
                if(WARN) log.warn("Unknown term: "+v);

                ntermsNotFound.incrementAndGet();

            }
            
        }
        
        // Verify statements (batch operation).
        if (numStmts > 0) {

            final SPO[] a = new SPO[numStmts];
            final BigdataStatement[] b = new BigdataStatement[numStmts];
            
            // #of SPOs generated for testing.
            int n = 0;
            
            for (int i = 0; i < numStmts; i++) {

                final BigdataStatement stmt = stmts[i];

                ntriples.incrementAndGet();

                if (!stmt.isFullyBound()) {

                    if (WARN)
                        log
                                .warn("Unknown statement (one or more unknown terms) "
                                        + stmt);

                    ntriplesNotFound.incrementAndGet();

                    continue;

                }

                a[n] = new SPO(stmt);
                
                b[n] = stmt;
                
                n++;
                
            }
            
            final IChunkedOrderedIterator<ISPO> itr = database
                    .bulkCompleteStatements(a, n);

            try {

                while (itr.hasNext()) {

                    itr.next();

                }

            } finally {

                itr.close();

            }
            
            for (int i = 0; i < n; i++) {

                final ISPO spo = a[i];

                if (!spo.hasStatementType()) {

                    ntriplesNotFound.incrementAndGet();

                    if(WARN)
                    log.warn("Statement not in database: " + b[i]+" ("+spo+")");

                    continue;

                }

                if (spo.getStatementType() != StatementEnum.Explicit) {
                    
                    ntriplesNotFound.incrementAndGet();

                    if(WARN)
                    log.warn("Statement not explicit database: "+b[i]+" is marked as "+spo.getStatementType());
                    
                    continue;
                    
                }
                
            }
            
        }
        
        // Reset the state of the buffer (but not the bnodes nor deferred stmts).
        _clear();

    }
    
}