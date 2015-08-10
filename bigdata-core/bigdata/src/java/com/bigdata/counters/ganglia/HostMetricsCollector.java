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
 * Reflects collected host metrics to ganglia.
 * 
 * <pre>
 * /192.168.1.10/CPU/% IO Wait=0.0
 * /192.168.1.10/CPU/% Processor Time=0.24
 * /192.168.1.10/CPU/% System Time=0.08
 * /192.168.1.10/CPU/% User Time=0.16
 * /192.168.1.10/Info/Architecture=amd64
 * /192.168.1.10/Info/Number of Processors=4
 * /192.168.1.10/Info/Operating System Name=Linux
 * /192.168.1.10/Info/Operating System Version=2.6.38-11-server
 * /192.168.1.10/Info/Processor Info=Intel(R) Core(TM) i7-2620M CPU @ 2.70GHz Family 6 Model 42 Stepping 7, GenuineIntel
 * /192.168.1.10/Memory/Bytes Free=5.031514112E9
 * /192.168.1.10/Memory/Major Page Faults Per Second=0.0
 * /192.168.1.10/Memory/Swap Bytes Used=0.0
 * /192.168.1.10/PhysicalDisk/Bytes Read Per Second=0.0
 * /192.168.1.10/PhysicalDisk/Bytes Written Per Second=6144.0
 * </pre>
 */
public class HostMetricsCollector implements IGangliaMetricsCollector {

    /**
     * Match anything which does NOT include <code>.service.</code> in the
     * counter path.
     * 
     * @see <a href="http://www.regular-expressions.info/lookaround.html">
     *      Regular Expressions Info </a>
     */
    static final Pattern filter; 
    static {

        filter = Pattern.compile("^(?!.*/service/).*$");
        
    }

	private final AbstractStatisticsCollector statisticsCollector;

	public HostMetricsCollector(
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

		// Start at the "host" level.
		final CounterSet counters = (CounterSet) statisticsCollector
				.getCounters().getPath(basePrefix);

		@SuppressWarnings("rawtypes")
        final Iterator<ICounter> itr = counters.getCounters(filter);

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
