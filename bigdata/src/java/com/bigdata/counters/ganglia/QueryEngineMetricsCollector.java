package com.bigdata.counters.ganglia;

import java.util.Iterator;
import java.util.regex.Pattern;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.ganglia.GangliaMunge;
import com.bigdata.ganglia.IGangliaMetricsCollector;
import com.bigdata.ganglia.IGangliaMetricsReporter;

/**
 * Reflects query engine metrics from the data server nodes to ganglia.
 */
public class QueryEngineMetricsCollector implements IGangliaMetricsCollector {

    /**
     * Match anything which does NOT include <code>.service.</code> in the
     * counter path.
     * 
     * @see <a href="http://www.regular-expressions.info/lookaround.html">
     *      Regular Expressions Info </a>
     */
    static final Pattern filter; 
    static {

        // bigdata10.bigdata.com / service /
        // com.bigdata.service.IMetadataService /
        // dabb5034-9db0-4218-a6dd-f9032fc05ce6 / Query Engine /
        // blockedWorkQueueCount

        filter = Pattern
                .compile("^.*/service/com.bigdata.service.IDataService/.*/Query Engine/.*$");
        
    }

	private final AbstractStatisticsCollector statisticsCollector;

	public QueryEngineMetricsCollector(
			final AbstractStatisticsCollector statisticsCollector) {

		if (statisticsCollector == null)
			throw new IllegalArgumentException();

		this.statisticsCollector = statisticsCollector;
		
	}

	@Override
	public void collect(final IGangliaMetricsReporter reporter) {

//		log.warn(statisticsCollector.getCounters().toString());

		// Common base path which is NOT included in the generated metric name.
		final String basePrefix = ICounterSet.pathSeparator
				+ AbstractStatisticsCollector.fullyQualifiedHostName
				+ ICounterSet.pathSeparator;

        /*
         * Total path prefix for all metrics to be reported.
         * 
         * Note: This includes the path components:
         * "/host/service/serviceIface/serviceUUID/"
         * 
         * Thus it will NOT report per-host metrics.
         */
        final String pathPrefix = basePrefix
                + statisticsCollector.getProcessName();

        final CounterSet counters = (CounterSet) statisticsCollector
                .getCounters().getPath(pathPrefix);

		@SuppressWarnings("rawtypes")
        final Iterator<ICounter> itr = counters.getCounters(filter);

		while (itr.hasNext()) {

			final ICounter<?> c = itr.next();

			final Object value = c.getInstrument().getValue();

			// The full path to the metric name.
			final String path = c.getPath();

            /*
             * By using pathPrefix, we remove the hostName, the service
             * interface type, and the service UUID from the counter path before
             * accepting the rest of the path as the metric name. As long as
             * there is one "key" service per node (CS, DS, etc), this provides
             * exact information. When there are multiple services on a node,
             * this will cause the information from those services to contend
             * rather than aggregate.
             * 
             * This works great for the QueryEngine metrics as long as there is
             * just one DataService per host.
             */
            final String s = path.substring(pathPrefix.length());
            
//            /*
//             * With this approach all metric names are unique since we are only
//             * removing the host name from the counter path. However, the
//             * presence of the UUID in the metric name makes it difficult to
//             * aggregate this stuff in the ganglia UI.
//             */
//            final String s = path.substring(basePrefix.length());

            // Just the metric name.
            final String metricName = GangliaMunge.munge(s).replace('/', '.');

			reporter.setMetric(metricName, value);

		}

	}
	
}
