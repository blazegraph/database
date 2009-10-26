package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.ICloseableIterator;

public class BigdataConstructIterator implements
        CloseableIteration<Statement, QueryEvaluationException> {
    private final AbstractTripleStore db;

    private final BigdataStatementIterator stmtIt;

    public BigdataConstructIterator(
            final AbstractTripleStore db,
            final CloseableIteration<? extends BindingSet, QueryEvaluationException> src) {
        assert db != null && src != null;
        this.db = db;
        /*
         * FIXME This must reuse the reverse blank nodes mapping for the
         * SailConnection to resolve blank node term identifiers to blank node
         * objects across the scope of the SailConnection.
         */
        stmtIt = db.asStatementIterator(db
                .bulkCompleteStatements(new ChunkedWrappedIterator<ISPO>(
                        new SPOConverter(src))));
    }

    public boolean hasNext() throws QueryEvaluationException {
//        try {
            return stmtIt.hasNext();
//        } catch (SailException ex) {
//            throw new QueryEvaluationException(ex);
//        }
    }

    public Statement next() throws QueryEvaluationException {
//        try {
            return stmtIt.next();
//        } catch (SailException ex) {
//            throw new QueryEvaluationException(ex);
//        }
    }

    public void remove() throws QueryEvaluationException {
        throw new UnsupportedOperationException();
    }

    public void close() throws QueryEvaluationException {
        
        stmtIt.close();
        
    }

    private class SPOConverter implements ICloseableIterator<ISPO> {
        
        private final CloseableIteration<? extends BindingSet, QueryEvaluationException> src;

        public SPOConverter(
                final CloseableIteration<? extends BindingSet, QueryEvaluationException> src) {
            
            this.src = src;
            
        }
        
        public void close() {
            try {
                src.close();
            } catch (QueryEvaluationException ex) {
                throw new RuntimeException(ex);
            }
        }

        public boolean hasNext() {
            try {
                return src.hasNext();
            } catch (QueryEvaluationException ex) {
                throw new RuntimeException(ex);
            }
        }

        public SPO next() {
            try {
                return convert(src.next());
            } catch (QueryEvaluationException ex) {
                throw new RuntimeException(ex);
            }
        }

        public void remove() {
            try {
                src.remove();
            } catch (QueryEvaluationException ex) {
                throw new RuntimeException(ex);
            }
        }

        private SPO convert(BindingSet bindingSet) {
            Value subject = bindingSet.getValue("subject");
            Value predicate = bindingSet.getValue("predicate");
            Value object = bindingSet.getValue("object");
            final long s;
            if (subject instanceof BigdataValue) {
                s = ((BigdataValue) subject).getTermId();
            } else {
                s = db.getTermId(subject);
            }
            final long p;
            if (predicate instanceof BigdataValue) {
                p = ((BigdataValue) predicate).getTermId();
            } else {
                p = db.getTermId(predicate);
            }
            final long o;
            if (object instanceof BigdataValue) {
                o = ((BigdataValue) object).getTermId();
            } else {
                o = db.getTermId(object);
            }
            SPO spo = new SPO(s, p, o);
            return spo;
        }
        
//        /**
//         * Don't really need chunking, but we do need to be closeable.
//         */
//        public SPO[] nextChunk() {
//            return nextChunk(null);
//        }
//
//        /**
//         * Don't really need chunking, but we do need to be closeable.
//         */
//        public SPOKeyOrder getKeyOrder() {
//            
//            return SPOKeyOrder.SPO;
//            
//        }
//
//        /**
//         * Don't really need chunking, but we do need to be closeable.
//         */
//        public SPO[] nextChunk(KeyOrder keyOrder) {
//            return new SPO[] { next() };
//        }
        
    }
}
