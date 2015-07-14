package com.bigdata.rdf.sail.remote;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;

import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

public class BigdataRemoteTupleQuery extends AbstractBigdataRemoteQuery implements TupleQuery {

	private IPreparedTupleQuery q;

	public BigdataRemoteTupleQuery(RemoteRepository remote, String query, String baseURI) throws Exception {
		super(baseURI);
		this.q = remote.prepareTupleQuery(query);
	}

	@Override
	public TupleQueryResult evaluate() throws QueryEvaluationException {
		try {
			configureConnectOptions(q);
			return q.evaluate();
		} catch (Exception ex) {
			throw new QueryEvaluationException(ex);
		}
	}

    /**
     * @see http://trac.blazegraph.com/ticket/914 (Set timeout on
     *      remote query)
     */
    @Override
	public int getMaxQueryTime() {

        final long millis = q.getMaxQueryMillis();

        if (millis == -1) {
            // Note: -1L is returned if the http header is not specified.
            return -1;
            
        }
        
        return (int) TimeUnit.MILLISECONDS.toSeconds(millis);

	}

    /**
     * @see http://trac.blazegraph.com/ticket/914 (Set timeout on
     *      remote query)
     */
    @Override
    public void setMaxQueryTime(final int seconds) {

        q.setMaxQueryMillis(TimeUnit.SECONDS.toMillis(seconds));

    }

	public void evaluate(TupleQueryResultHandler handler)
			throws QueryEvaluationException, TupleQueryResultHandlerException {
		TupleQueryResult tqr = evaluate();
		try {
			handler.startQueryResult(tqr.getBindingNames());
			while (tqr.hasNext()) {
				handler.handleSolution(tqr.next());
			}
			handler.endQueryResult();
		} finally {
			tqr.close();
		}
	}
}
