package com.bigdata.rdf.sail.remote;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.BooleanQuery;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.rdf.sail.webapp.client.IPreparedBooleanQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

public class BigdataRemoteBooleanQuery extends AbstractBigdataRemoteQuery implements BooleanQuery {

	private IPreparedBooleanQuery q;

	public BigdataRemoteBooleanQuery(RemoteRepository remote, String query, String baseURI) throws Exception {
		super(baseURI);
		this.q = remote.prepareBooleanQuery(query);
	}

	@Override
	public boolean evaluate() throws QueryEvaluationException {
		try {
			configureConnectOptions(q);
			return q.evaluate();
		} catch (Exception ex) {
			throw new QueryEvaluationException(ex);
		}
	}

	/**
     * @see http://trac.blazegraph.com/ticket/914 (Set timeout on remote query)
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
     * @see http://trac.blazegraph.com/ticket/914 (Set timeout on remote query)
     */
	@Override
	public void setMaxQueryTime(final int seconds) {

	    q.setMaxQueryMillis(TimeUnit.SECONDS.toMillis(seconds));
	    
	}

}