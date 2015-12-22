package com.bigdata.rdf.sail.remote;

import java.util.concurrent.TimeUnit;

import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import com.bigdata.rdf.sail.webapp.client.IPreparedGraphQuery;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;

public class BigdataRemoteGraphQuery extends AbstractBigdataRemoteQuery implements GraphQuery {

	private IPreparedGraphQuery q;

	public BigdataRemoteGraphQuery(RemoteRepository remote, String query, String baseURI) throws Exception {
		super(baseURI);
		this.q = remote.prepareGraphQuery(query);
	}
		
		@Override
		public GraphQueryResult evaluate() throws QueryEvaluationException {
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

		@Override
		public void evaluate(RDFHandler handler) throws QueryEvaluationException,
				RDFHandlerException {
			GraphQueryResult gqr = evaluate();
			try {
				handler.startRDF();
				while (gqr.hasNext()) {
					handler.handleStatement(gqr.next());
				}
				handler.endRDF();;
			} finally {
				gqr.close();
			}
		}
}
