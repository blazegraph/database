/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on May 9, 2008
 */

package com.bigdata.counters.linux;

import com.bigdata.counters.*;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestParsing extends TestCase2 {

    public TestParsing() {
        super();
    }

    public TestParsing(String name) {
        super(name);
    }
    
    /**
     * Test for {@link SysstatUtil#splitDataLine(String)}.
     */
    public void test_splitDataLine01(){

        final String header = "06:35:15        PID   %user %system    %CPU   CPU  Command";
        
        final String[] fields = SysstatUtil.splitDataLine(header);

		if (log.isInfoEnabled())
			log.info(Arrays.toString(fields));
        
        assertEquals(new String[] { "06:35:15", "PID", "%user", "%system",
                "%CPU", "CPU", "Command" }, fields);

    }
    
    /**
     * Test for {@link SysstatUtil#splitDataLine(String)}.
     */
    public void test_splitDataLine02(){

        final String data   = "06:35:15        501    0.00    0.01    0.00     1  kjournald";
        
        final String[] fields = SysstatUtil.splitDataLine(data);

        if(log.isInfoEnabled())
        	log.info(Arrays.toString(fields));
        
        assertEquals(new String[] {"06:35:15", "501", "0.00", "0.01", "0.00", "1", "kjournald"}, fields);
        
    }

    /**
     * Test for {@link SysstatUtil#splitDataLine(String)}.
     */
    public void test_splitDataLineLeadingSpaces(){

        final String header = " 06:35:15        PID   %user %system    %CPU   CPU  Command";

        final String[] fields = SysstatUtil.splitDataLine(header);

        if (log.isInfoEnabled())
            log.info(Arrays.toString(fields));

        System.out.println(Arrays.toString(fields));
        assertEquals(new String[] { "06:35:15", "PID", "%user", "%system",
                "%CPU", "CPU", "Command" }, fields);

    }
    /**
     * Test parse of the sysstat ISO date format.
     * 
     * @todo test only writes on the console - you need to verify the outcome by
     *       hand.
     */
    public void test_pidStat_data_parse() {

        final DateFormat f = SysstatUtil.newDateFormat();

        System.err.println("Format: " + f.format(new Date()));

        try {

            System.err.println("Parsed: " + f.parse("06:35:15 AM"));

            System.err.println("Parsed: " + f.parse("02:08:24 PM"));

        } catch (ParseException e) {

            log.error("Could not parse?");

        }

    }

    /**
     * Test based on some sample data.
     */
    public void test_vmstat_header_and_data_parse() {
       
        final Pattern pattern = VMStatCollector.pattern;

        final String header =//
            "  r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st";

        final String data =//
            "  1  0     96 178580 206520 1170604   56   12     0     7    1    0  1  0 99  3  0";

        // test header parse.
        {
            final String[] fields = pattern.split(header.trim(), 0/* limit */);

            for (int i = 0; i < fields.length; i++) {

            	if(log.isInfoEnabled())
                	log.info("fields[" + i + "]=[" + fields[i] + "]");
                
            }
            
            assertField(header, fields, 2, "swpd");
            assertField(header, fields, 3, "free");

            assertField(header, fields, 6, "si");
            assertField(header, fields, 7, "so");

            assertField(header, fields, 12, "us");
            assertField(header, fields, 13, "sy");
            assertField(header, fields, 14, "id");
            assertField(header, fields, 15, "wa");
        }
        
        // test data parse.
        {
            final String[] fields = pattern.split(data.trim(), 0/* limit */);

            assertField(data, fields, 2, "96");
            assertField(data, fields, 3, "178580");

            assertField(data, fields, 6, "56");
            assertField(data, fields, 7, "12");

            assertField(data, fields, 12, "1");
            assertField(data, fields, 13, "0");
            assertField(data, fields, 14, "99");
            assertField(data, fields, 15, "3");
        }

    }

    public void test_get_data_map() {
        String header = "15:55:52      UID       PID    %usr %system  %guest    %CPU   CPU  Command\n";
        String data = "15:55:54     1000      3308    0.50    0.00    0.00    0.25     0  java\n";

        Map<String, String> fields = SysstatUtil.getDataMap(header, data);
        assertEquals(fields.get("%usr"), "0.50");
        assertEquals(fields.size(), 9);
    }

    public void test_get_data_incorrect() {
        String header = "15:55:52      UID       PID    %usr %system  %guest    %CPU   CPU\n";
        String data = "15:55:54     1000      3308    0.50    0.00    0.00    0.25     0  java\n";

        try {
            Map<String, String> fields = SysstatUtil.getDataMap(header, data);
            assertTrue("Exception should be thrown", false);
        } catch (IllegalArgumentException e ) {
            e.printStackTrace();
        }
    }

    /**
     * Used to verify that the header corresponds to our expectations. Logs
     * errors when the expectations are not met.
     * 
     * @param header
     *            The header line.
     * @param fields
     *            The fields parsed from that header.
     * @param field
     *            The field number in [0:#fields-1].
     * @param expected
     *            The expected value of the header for that field.
     */
    static protected void assertField(final String header,
            final String[] fields, final int field, final String expected) {

        if (header == null)
            throw new IllegalArgumentException();

        if (fields == null)
            throw new IllegalArgumentException();

        if (expected == null)
            throw new IllegalArgumentException();

        if (field < 0)
            throw new IllegalArgumentException();

        if (field >= fields.length)
            throw new AssertionFailedError("There are only " + fields.length
                    + " fields, but field=" + field + "\n" + header);

        if (!expected.equals(fields[field])) {

            throw new AssertionFailedError("Expected field=" + field
                    + " to be [" + expected + "], actual=" + fields[field]
                    + "\n" + header);

        }

    }

    public void test_pid_stat_collector_valid() throws IOException, InterruptedException {
        final String output =
                "Linux 3.16.0-4-amd64 (hostname)     27.10.2015      _x86_64_        (4 CPU)\n" +
                        "\n" +
                        "15:55:52      UID       PID    %usr %system  %guest    %CPU   CPU  Command\n" +
                        "15:55:54     1000      3308    0.50    0.00    0.00    0.25     0  java\n" +
                        "\n" +
                        "15:55:52      UID       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command\n" +
                        "15:55:54     1000      3308      8.00      0.00 2825204 1289368  15.73  java\n" +
                        "\n";

        test_pid_stat_collector(output);
    }


    public void test_pid_stat_collector_extra_column() throws IOException, InterruptedException {
        final String output =
                "Linux 3.16.0-4-amd64 (hostname)     27.10.2015      _x86_64_        (4 CPU)\n" +
                        "\n" +
                        "15:55:52  somecolumn    UID       PID    %usr %system  %guest    %CPU   CPU  Command\n" +
                        "15:55:54      100500   1000      3308    0.50    0.00    0.00    0.25     0  java\n" +
                        "\n" +
                        "15:55:52      UID       PID  minflt/s  majflt/s     VSZ    RSS   %MEM  Command\n" +
                        "15:55:54     1000      3308      8.00      0.00 2825204 1289368  15.73  java\n" +
                        "\n";

        test_pid_stat_collector(output);
    }

    protected void test_pid_stat_collector(String output) throws IOException, InterruptedException {


        final PIDStatCollector pidStatCollector = new PIDStatCollector(1, 1, new KernelVersion("2.6.32")) {
            @Override
            public AbstractProcessReader getProcessReader() {
                return new PIDStatReader() {
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
        final PIDStatCollector.PIDStatReader pidStatReader = (PIDStatCollector.PIDStatReader) pidStatCollector.getProcessReader();
        pidStatReader.start(new ByteArrayInputStream(output.getBytes()));
        Thread t = new Thread(pidStatReader);
        CounterSet counterSet;
        try {
            t.start();
            Thread.sleep(100);
            counterSet = pidStatCollector.getCounters();
        } finally {
            t.interrupt();
        }

        double cpu_usr = (Double)((ICounter) counterSet.getChild(IProcessCounters.CPU).getChild("% User Time")).getInstrument().getValue();
        long rss = (Long)((ICounter) counterSet.getChild(IProcessCounters.Memory).getChild("Resident Set Size")).getInstrument().getValue();

        assertEquals(cpu_usr, 0.005d);
        assertEquals(rss, 1289368*1024);

    }
}
