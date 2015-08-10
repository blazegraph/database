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
 * Reflects collected service metrics to ganglia.
 * 
 * <pre>
 * /192.168.1.10/test-context-7/CPU/% Processor Time=0.0
 * /192.168.1.10/test-context-7/CPU/% System Time=0.0
 * /192.168.1.10/test-context-7/CPU/% User Time=0.0020
 * /192.168.1.10/test-context-7/Memory/Major Faults per Second=0.0
 * /192.168.1.10/test-context-7/Memory/Minor Faults per Second=0.0
 * /192.168.1.10/test-context-7/Memory/Percent Memory Size=0.0091
 * /192.168.1.10/test-context-7/Memory/Resident Set Size=76304384
 * /192.168.1.10/test-context-7/Memory/Virtual Size=2449391616
 * /192.168.1.10/test-context-7/PhysicalDisk/Bytes Read per Second=0.0
 * /192.168.1.10/test-context-7/PhysicalDisk/Bytes Written per Second=0.0
 * </pre>
 */
public class ServiceMetricsCollector implements IGangliaMetricsCollector {

	final protected AbstractStatisticsCollector statisticsCollector;
	final protected Pattern filter;

	public ServiceMetricsCollector(
			final AbstractStatisticsCollector statisticsCollector,
			final Pattern filter) {

		if (statisticsCollector == null)
			throw new IllegalArgumentException();

		this.statisticsCollector = statisticsCollector;

		this.filter = filter;

	}

	@Override
	public void collect(final IGangliaMetricsReporter reporter) {

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
             * rather than aggregate. Since there is typically an SMS also
             * running on the node, the SMS should not be reporting out these
             * counters or it could overwrite the main services counters when
             * they are combined in gmetad.
             * 
             * TODO A better approach would be to match and remove the UUID from
             * the counter name. That way the SMS and DS/CS/MDS metrics would
             * have different prefixes but different DS instances on different
             * nodes would use the same name for a given performance counter.
             * 
             * / bigdata10.bigdata.com / service /
             * com.bigdata.service.IMetadataService /
             * f4c2ee13-0679-494d-913d-76d14f7d0385 / ...
             */
//            final String s = path.substring(pathPrefix.length());
            
            /*
             * With this approach all metric names are unique since we are only
             * removing the host name from the counter path. However, the
             * presence of the UUID in the metric name makes it difficult to
             * aggregate this stuff in the ganglia UI.
             */
            final String s = path.substring(basePrefix.length());

			// Just the metric name.
			final String metricName = GangliaMunge.munge(s).replace('/', '.');

			reporter.setMetric(metricName, value);

		}

	}

}
