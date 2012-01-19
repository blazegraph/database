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

		// Total path prefix for all metrics to be reported.
		final String pathPrefix = basePrefix
				+ statisticsCollector.getProcessName();

		final CounterSet serviceCounters = (CounterSet) statisticsCollector
				.getCounters().getPath(pathPrefix);

		@SuppressWarnings("rawtypes")
		final Iterator<ICounter> itr = serviceCounters.getCounters(filter);

		while (itr.hasNext()) {

			final ICounter<?> c = itr.next();

			final Object value = c.getInstrument().getValue();

			// The full path to the metric name.
			final String path = c.getPath();

			// Just the metric name.
			final String metricName = GangliaMunge.munge(path.substring(
					basePrefix.length()).replace('/', '.'));

			reporter.setMetric(metricName, value);

		}

	}

}
