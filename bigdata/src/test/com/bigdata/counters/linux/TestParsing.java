/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import junit.framework.TestCase2;

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

        final String header = "06:35:15 AM       PID   %user %system    %CPU   CPU  Command";
        
        final String[] fields = SysstatUtil.splitDataLine(header);

        System.err.println(Arrays.toString(fields));
        
        assertEquals(new String[] { "06:35:15 AM", "PID", "%user", "%system",
                "%CPU", "CPU", "Command" }, fields);

    }
    
    /**
     * Test for {@link SysstatUtil#splitDataLine(String)}.
     */
    public void test_splitDataLine02(){

        final String data   = "06:35:15 AM       501    0.00    0.01    0.00     1  kjournald";
        
        final String[] fields = SysstatUtil.splitDataLine(data);

        System.err.println(Arrays.toString(fields));
        
        assertEquals(new String[] {"06:35:15 AM", "501", "0.00", "0.01", "0.00", "1", "kjournald"}, fields);
        
    }
    
    /**
     * Test parse of the sysstat ISO date format.
     * 
     * @todo test only writes on the console - you need to verify the outcome by
     *       hand.
     */
    public void test_pidStat_data_parse() {

        DateFormat f = SysstatUtil.newDateFormat();

        System.err.println("Format: " + f.format(new Date()));

        try {

            System.err.println("Parsed: " + f.parse("06:35:15 AM"));

            System.err.println("Parsed: " + f.parse("02:08:24 PM"));

        } catch (ParseException e) {

            log.error("Could not parse?");

        }

    }

}
