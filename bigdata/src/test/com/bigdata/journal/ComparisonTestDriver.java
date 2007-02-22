/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Feb 21, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.bigdata.journal.StressTestConcurrent.TestOptions;
import com.bigdata.rawstore.Bytes;

/**
 * A harness for running comparison of different journal configurations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ComparisonTestDriver {

    /**
     * Interface for tests that can be run by {@link ComparisonTestDriver}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IComparisonTest {
        
        /**
         * Run a test.
         * 
         * @param properties
         *            The properties used to configure the test.
         * 
         * @return The test result to report.
         */
        public String doComparisonTest(Properties properties) throws Exception;
        
    }
    
    /**
     * A name-value pair used to override {@link Properties} for a {@link Condition}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class NV {
        public final String name;
        public final String value;
        public NV(String name,Object value) {
            this.name = name;
            this.value = value.toString();
        }
        public int hashCode() {
            return name.hashCode();
        }
        public boolean equals(NV o) {
            return name.equals(o.name) && value.equals(o.value);
        }
    }
    
    /**
     * An experimental condition.
     * 
     * @todo record the results.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class Condition {
        public final String name;
        public final Properties properties;
        public String result;
        public Condition(String name,Properties properties) {
            this.name = name;
            this.properties = properties;
        }
        
    }
    
    /**
     * Return a {@link Properties} object that inherits defaults from
     * <i>properties</i> and sets/overrides properties identified in <i>entries</i>.
     * 
     * @param properties
     *            The inherited properties (this object is NOT modified).
     * @param entries
     *            The overriden properties.
     *            
     * @return A new {@link Properties}.
     */
    protected static Condition getCondition(Properties properties, NV[] entries) throws IOException {

        properties = new Properties(properties);
        
        StringBuilder sb = new StringBuilder();
        
//        sb.append("{");
        
        for(int i=0; i<entries.length; i++) {

            if(i>0) sb.append("; "); // Note: Not a comma since CSV delimited.
            
            sb.append(entries[i].name+"="+entries[i].value);
           
            properties.setProperty(entries[i].name,entries[i].value);
            
        }

//        sb.append("}");

        String name = sb.toString();

        return new Condition(name,properties);
        
    }

    static public List<Condition> getBasicConditions(Properties properties, NV[] params) throws Exception {

        properties = new Properties(properties);
        
        for(int i=0; i<params.length; i++) {
            
            properties.setProperty(params[i].name,params[i].value);
            
        }
        
        Condition[] conditions = new Condition[] { //
                getCondition(properties, new NV[] { //
                        new NV(Options.BUFFER_MODE, BufferMode.Transient), //
                        }), //
//                getCondition(
//                        properties,
//                        new NV[] { //
//                                new NV(Options.BUFFER_MODE,
//                                        BufferMode.Transient), //
//                                new NV(Options.USE_DIRECT_BUFFERS, Boolean.TRUE) //
//                        }), //
                getCondition(properties, new NV[] { //
                        new NV(Options.BUFFER_MODE, BufferMode.Direct), //
                        }), //
//                getCondition(
//                        properties,
//                        new NV[] { //
//                                new NV(Options.BUFFER_MODE, BufferMode.Direct), //
//                                new NV(Options.USE_DIRECT_BUFFERS, Boolean.TRUE) //
//                        }), //
                getCondition(properties, new NV[] { //
                        new NV(Options.BUFFER_MODE, BufferMode.Direct), //
                                new NV(Options.FORCE_ON_COMMIT, ForceEnum.No) //
                        }), //
                getCondition(properties, new NV[] { //
                        new NV(Options.BUFFER_MODE, BufferMode.Disk), //
                        }), //
                getCondition(properties, new NV[] { //
                        new NV(Options.BUFFER_MODE, BufferMode.Disk), //
                                new NV(Options.FORCE_ON_COMMIT, ForceEnum.No) //
                        }), //
        };
        
        return Arrays.asList(conditions);

    }
    
    /**
     * Runs a comparison of various an {@link IComparisonTest} under various
     * conditions and writes out a summary of the reported results.
     * 
     * @param args
     *            The name of a class that implements IComparisonTest.
     * 
     * @todo this is not really parameterized very well for the className since
     *       some of the options are specific to the test class.
     * 
     * @todo it would be nice to factor out the column names for the results.
     *       Perhaps change from a String result to a NVPair<String,String> and
     *       add something to {@link IComparisonTest} to declare the headings?
     * 
     * @todo Optional name of a properties file to be read. the properties will
     *       be used as the basis for all conditions.
     */
    public static void main(String[] args) throws Exception {

        String className = args[0];
        
        if(className==null) {
            
            className = StressTestConcurrent.class.getName();
            
        }
        
        File outFile = new File(className+".comparison.csv");
        
        if(outFile.exists()) throw new IOException("File exists: "+outFile.getAbsolutePath());

        Properties properties = new Properties();

        // force delete of the files on close of the journal under test.
        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
//        properties.setProperty(Options.DELETE_ON_CLOSE,"true");
        properties.setProperty(Options.SEGMENT, "0");

        // avoids journal overflow when running out to 60 seconds.
        properties.setProperty(Options.MAXIMUM_EXTENT, ""+Bytes.megabyte32*400);
        
        properties.setProperty(TestOptions.TIMEOUT,"30");

        List<Condition>conditions = new ArrayList<Condition>();

        conditions.addAll(getBasicConditions(properties, new NV[] { new NV(
                TestOptions.NCLIENTS, "1") }));

        conditions.addAll(getBasicConditions(properties, new NV[] { new NV(
                TestOptions.NCLIENTS, "2") }));

        conditions.addAll(getBasicConditions(properties, new NV[] { new NV(
                TestOptions.NCLIENTS, "10") }));

        conditions.addAll(getBasicConditions(properties, new NV[] { new NV(
                TestOptions.NCLIENTS, "20") }));

        conditions.addAll(getBasicConditions(properties, new NV[] { new NV(
                TestOptions.NCLIENTS, "100") }));

        conditions.addAll(getBasicConditions(properties, new NV[] { new NV(
                TestOptions.NCLIENTS, "200") }));

        // properties.setProperty(Options.BUFFER_MODE,
        // BufferMode.Mapped.toString());

        final int nconditions = conditions.size();

        {

            FileWriter writer = new FileWriter(outFile);
            
            System.err.println("Running comparison of " + nconditions
                    + " conditions for " + className);

            Class cl = Class.forName(className);

            Iterator<Condition> itr = conditions.iterator();

            while (itr.hasNext()) {

                Condition condition = itr.next();

                IComparisonTest test = (IComparisonTest) cl.newInstance();

                System.err.println("Running: "+ condition.name);

                try {
                    condition.result = test
                            .doComparisonTest(condition.properties);
                } catch (Exception ex) {
                    condition.result = ex.getMessage();
                }

                System.err.println(condition.result + ", " + condition.name);

                writer.write(condition.result + ", " + condition.name+"\n");

            }
            
            writer.flush();
            
            writer.close();

        }

        {
        
            System.err.println("Result summary:");

            Iterator<Condition> itr = conditions.iterator();

            while (itr.hasNext()) {

                Condition condition = itr.next();

                System.err.println(condition.result + ", " + condition.name);

            }

        }

    }

}
