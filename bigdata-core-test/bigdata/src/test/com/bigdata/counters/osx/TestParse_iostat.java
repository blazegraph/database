package com.bigdata.counters.osx;

import com.bigdata.counters.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * OSX does have an <code>iostat</code>. This is not the same as the utility we
 * support under linux. <code>iostat -d -C -n 999 -w 60</code> provides an
 * update every 60 seconds. The -d tells it to report just device statistics (it
 * will report CPU and load average statistics as well by default). The "-C"
 * option tells it that you want the CPU stats anyway (this includes IOWait). (A
 * "-U" option may be used to explicitly request the load average stats.) The
 * "-n 999" tells it to display up to 999 devices. Otherwise it will truncate
 * the output at 80 columns.
 * 
 * <pre>
 *           disk0           disk1           disk2           disk3           disk4       cpu
 *     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id
 *   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63
 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  53  4 43
 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  56  8 37
 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  54  9 37
 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  57  8 35
 * </pre>
 * 
 * The output of this command depends on the #of devices which are attached, and
 * that can actually change over time (device connect/disconnect). Unless we are
 * going to aggregate across the devices reported in each time period it might
 * make more sense to only use this for the CPU stats, which can be done by
 * specifying "-n 0" for NO devices.
 * 
 * <pre>
 *  iostat -d -C -n 0 -w 1 | cat
 *       cpu
 *  us sy id
 *  31  6 63
 *  57  8 35
 *  57  6 37
 * </pre>
 * 
 * Regardless, the headers will repeat periodically.
 * 
 * @author thompsonbry@users.sourceforge.net
 */
public class TestParse_iostat extends AbstractParserTestCase {

	public TestParse_iostat() {
		super();
	}
	
	public TestParse_iostat(String name) {
		super(name);
	}

	/**
	 * Test parsing of a CPU only report from
	 * <code>iostat -d -C -n 0 -w 1 | cat</code>
	 */
	public void test_parsing_iostat_cpu_only() {

		final String h0 = "        cpu";
		final String h1 = "   us sy id";
		final String d0 = "   38  7 55";

		final Pattern pattern = IOStatCollector.pattern;

		// test 1st header parse.
		{

			final String[] fields = pattern.split(h0.trim(), 0/* limit */);

			for (int i = 0; i < fields.length; i++) {

				if (log.isInfoEnabled())
					log.info("fields[" + i + "]=[" + fields[i] + "]");

			}

			assertField(h0, fields, 0, "cpu");
			assertEquals(1, fields.length);

		}

		// test 2nd header parse.
		{

			final String[] fields = pattern.split(h1.trim(), 0/* limit */);

			for (int i = 0; i < fields.length; i++) {

				if (log.isInfoEnabled())
					log.info("fields[" + i + "]=[" + fields[i] + "]");

			}

			assertField(h1, fields, 0, "us");
			assertField(h1, fields, 1, "sy");
			assertField(h1, fields, 2, "id");
			assertEquals(3, fields.length);

		}

		// test data parse.
		{

			final String[] fields = pattern.split(d0.trim(), 0/* limit */);

			for (int i = 0; i < fields.length; i++) {

				if (log.isInfoEnabled())
					log.info("fields[" + i + "]=[" + fields[i] + "]");

			}

			assertField(d0, fields, 0, "38");
			assertField(d0, fields, 1, "7");
			assertField(d0, fields, 2, "55");
			assertEquals(3, fields.length);

		}

    }

	/**
	 * Test parsing of the output of <code>iostat -d -C -n 999 -w 60</code>
	 * 
	 * <pre>
	 *           disk0           disk1           disk2           disk3           disk4       cpu
	 *     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id
	 *   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  53  4 43
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  56  8 37
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  54  9 37
	 *     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  57  8 35
	 * </pre>
	 * 
	 * Note that the #of device columns will vary for this format. The headers
	 * will also periodically repeat. The #of columns for the report can
	 * presumably vary as devices are connected and disconnected!
	 */
	public void test_parsing_iostat_devices_plus_cpu() {

		final String h0 = "        disk0           disk1           disk2           disk3           disk4       cpu";
		final String h1 = "     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id";
		final String d0 = "   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63";

		final Pattern pattern = IOStatCollector.pattern;

		// test 1st header parse.
		{

			final String[] fields = pattern.split(h0.trim(), 0/* limit */);

			for (int i = 0; i < fields.length; i++) {

				if (log.isInfoEnabled())
					log.info("fields[" + i + "]=[" + fields[i] + "]");

			}

			assertField(h0, fields, 0, "disk0");
			assertField(h0, fields, 1, "disk1");
			assertField(h0, fields, 2, "disk2");
			assertField(h0, fields, 3, "disk3");
			assertField(h0, fields, 4, "disk4");
			assertField(h0, fields, 5, "cpu");
			assertEquals(6, fields.length);

		}

		// test 2nd header parse.
		{
//		final String h0 = "        disk0           disk1           disk2           disk3           disk4       cpu";
//		final String h1 = "     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id";
//		final String d0 = "   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63";

			final String[] fields = pattern.split(h1.trim(), 0/* limit */);

			for (int i = 0; i < fields.length; i++) {

				if (log.isInfoEnabled())
					log.info("fields[" + i + "]=[" + fields[i] + "]");

			}

			assertField(h1, fields, 0, "KB/t");
			assertField(h1, fields, 1, "tps");
			assertField(h1, fields, 2, "MB/s");
			
			assertField(h1, fields, 3, "KB/t");
			assertField(h1, fields, 4, "tps");
			assertField(h1, fields, 5, "MB/s");
			
			assertField(h1, fields, 6, "KB/t");
			assertField(h1, fields, 7, "tps");
			assertField(h1, fields, 8, "MB/s");
			
			assertField(h1, fields, 9, "KB/t");
			assertField(h1, fields,10, "tps");
			assertField(h1, fields,11, "MB/s");
			
			assertField(h1, fields,12, "KB/t");
			assertField(h1, fields,13, "tps");
			assertField(h1, fields,14, "MB/s");
			
			assertField(h1, fields,15, "us");
			assertField(h1, fields,16, "sy");
			assertField(h1, fields,17, "id");
			
			assertEquals(18, fields.length);

		}

		// test data parse.
		{
//		final String h0 = "        disk0           disk1           disk2           disk3           disk4       cpu";
//		final String h1 = "     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id";
//		final String d0 = "   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63";

			final String[] fields = pattern.split(d0.trim(), 0/* limit */);

			for (int i = 0; i < fields.length; i++) {

				if (log.isInfoEnabled())
					log.info("fields[" + i + "]=[" + fields[i] + "]");

			}

			assertField(d0, fields, 0, "197.14");
			assertField(d0, fields, 1, "26");
			assertField(d0, fields, 2, "5.10");

			assertField(d0, fields, 3, "79.10");
			assertField(d0, fields, 4, "1");
			assertField(d0, fields, 5, "0.07");

			assertField(d0, fields, 6, "3.74");
			assertField(d0, fields, 7, "0");
			assertField(d0, fields, 8, "0.00");

			assertField(d0, fields, 9, "41.31");
			assertField(d0, fields,10, "0");
			assertField(d0, fields,11, "0.00");

			assertField(d0, fields,12, "13.03");
			assertField(d0, fields,13, "0");
			assertField(d0, fields,14, "0.00");

			assertField(d0, fields,15, "31");
			assertField(d0, fields,16, "6");
			assertField(d0, fields,17, "63");
			
			assertEquals(18, fields.length);

		}

	}
    public void test_iostat_collector_correct() throws IOException, InterruptedException {
        String output =
                "           disk0           disk1           disk2           disk3           disk4       cpu\n" +
                "     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s     KB/t tps  MB/s  us sy id\n" +
                "   197.14  26  5.10    79.10   1  0.07     3.74   0  0.00    41.31   0  0.00    13.03   0  0.00  31  6 63\n" +
                "     0.00   5  0.00     0.00   5  0.00     0.00   0  0.00     0.00   0  0.00     0.00   0  0.00  53  4 43\n";
        test_iostat_collector(output);
    }

    protected void test_iostat_collector(String output) throws IOException, InterruptedException {

        final IOStatCollector ioStatCollector = new IOStatCollector(1, true) {
            @Override
            public AbstractProcessReader getProcessReader() {
                return new IOStatReader() {
                    @Override
                protected ActiveProcess getActiveProcess() {
                        return new ActiveProcess() {
                            @Override
                            public boolean isAlive() {
                                return true;
                            }
                        };
                    }
                };
            }
        };
        final IOStatCollector.IOStatReader ioStatReader = (IOStatCollector.IOStatReader) ioStatCollector.getProcessReader();
        ioStatReader.start(new ByteArrayInputStream(output.getBytes()));
        Thread t = new Thread(ioStatReader);
        CounterSet counterSet;
        try {
            t.start();
            Thread.sleep(100);
            counterSet = ioStatCollector.getCounters();
        } finally {
            t.interrupt();
        }

        double cpu_usr = (Double)((ICounter) counterSet.getChild(IProcessCounters.CPU).getChild("% User Time")).getInstrument().getValue();
        double tps = (Double)((ICounter) counterSet.getChild(IProcessCounters.PhysicalDisk).getChild("Transfers Per Second")).getInstrument().getValue();

        assertEquals(0.53, cpu_usr);
        assertEquals(tps, 10.0);



        /*Iterator<ICounter> counters = counterSet.getCounters(Pattern.compile(".*"));
        while (counters.hasNext()) {
            System.err.println(counters.next().getInstrument().getValue().toString());
        }*/

    }
}
