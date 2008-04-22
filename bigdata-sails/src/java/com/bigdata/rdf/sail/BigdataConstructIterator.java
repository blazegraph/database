package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIteratorImpl;

public class BigdataConstructIterator implements
        CloseableIteration<Statement, QueryEvaluationException> {
    private final AbstractTripleStore db;

    private final CloseableIteration<SPO, QueryEvaluationException> src;

    public BigdataConstructIterator(
            final AbstractTripleStore db,
            final CloseableIteration<? extends BindingSet, QueryEvaluationException> src) {
        assert db != null && src != null;
        this.db = db;
        this.src =
                new ConvertingIteration<BindingSet, SPO, QueryEvaluationException>(
                        src) {
                    @Override
                    public SPO convert(BindingSet bindingSet) {
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
                };
    }

    public boolean hasNext() throws QueryEvaluationException {
        return src.hasNext();
    }

    /**
     * @todo make this chunkified - collect up SPOs and complete them in bulk.
     * I will do this tomorrow.
     */
    public Statement next() throws QueryEvaluationException {
        SPO spo = src.next();
        ISPOIterator it = db.bulkCompleteStatements(new SPO[] { spo }, 1);
        return new BigdataStatementIteratorImpl(db, it).next();
    }

    public void remove() throws QueryEvaluationException {
        src.remove();
    }

    public void close() throws QueryEvaluationException {
        src.close();
    }
}
