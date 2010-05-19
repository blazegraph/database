/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 21, 2007
 */

package com.bigdata.test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase2;

import org.CognitiveWeb.util.PropertyUtil;
import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.ext.DefaultHandler2;
import org.xml.sax.ext.EntityResolver2;

import com.bigdata.journal.ProxyTestCase;
import com.bigdata.util.NV;

/**
 * A harness for running comparison of different configurations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ExperimentDriver {

    /**
     * Logger.
     */
    protected static final Logger log = Logger.getLogger(ExperimentDriver.class);
    
    protected static final boolean INFO = log.isInfoEnabled();

    /**
     * Interface for a result, which is a set of ordered name-value pairs.
     */
    public static class Result extends TreeMap<String,String>{
        
        /**
         * 
         */
        private static final long serialVersionUID = 5726970069639981728L;
     
        /**
         * Converts an exception into a result containing an "Error" column
         * having {@link Throwable#getMessage()} as its value and a "StackTrace"
         * colukn and having the stack trace (with newlines replaced by ";" and
         * carrige returns and tabs removed) as its value.
         * 
         * @param t
         *            The exception.
         */
        public static Result errorFactory(Throwable t) {
            
            StringWriter w = new StringWriter();
            
            t.printStackTrace(new PrintWriter(w));
            
            String trace = w.toString();
            
            trace = trace.replace("\n", "; ");
            trace = trace.replace("\r", "");
            trace = trace.replace("\t", "");
            
            Result result = new Result();
            
            result.put("Error", t.getMessage() );

            result.put("StackTrace", trace );
            
            return result;
            
        }

        public String toString() {
            
            return toString(false);
            
        }
        
        /**
         * Converts to a human readable representation using {name=value, ...}.
         * The attributes are listed in sorted order.
         */
        public String toString(boolean newline) {
            
            StringBuilder sb = new StringBuilder();
            
            Iterator<Map.Entry<String,String>> itr = entrySet().iterator();
            
            boolean first = true;
            
            while(itr.hasNext()) {
                
                if(!first) {
                    
                    sb.append(newline?"\n":", ");
                    
                }
                
                Map.Entry<String,String> entry = itr.next();
                
                sb.append(entry.getKey()+"="+entry.getValue());
                
                first = false;
                
            }
            
            return sb.toString();
            
        }
        
    }
    
    /**
     * Interface for tests that can be run by {@link ExperimentDriver}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IComparisonTest {
        
        /**
         * ala JUnit, but allows us to run {@link ProxyTestCase}s as well.
         * 
         * @param properties May be used to configure the test fixture.
         * 
         * @throws Exception
         */
        public void setUpComparisonTest(Properties properties) throws Exception;
        
        /**
         * Run a test.
         * 
         * @param properties
         *            The properties used to configure the test.
         * 
         * @return The test result to report.
         */
        public Result doComparisonTest(Properties properties) throws Exception;
        
        /**
         * ala JUnit, but allows us to run {@link ProxyTestCase}s as well.
         * 
         * @throws Exception
         */
        public void tearDownComparisonTest() throws Exception;
        
    }
    
    /**
     * Converts a map to human readable representation using a tab delimited
     * format. The attributes are listed in order by the given column names. If
     * an attribute is not present then an empty cell is output.
     * 
     * @param cols
     *            The column names.
     * @param showAll
     *            When true, any attributes not named in <i>cols</i> are added
     *            after the last column named in <i>cols</i>. Since there are
     *            no declared columns for these attributes they are written as
     *            [name=value].
     */
    static public String toString(Map<? extends Object,? extends Object> data,String[] cols,boolean showAll) {
        
        final StringBuilder sb = new StringBuilder();
        
        /*
         * Build a map in which all keys and values are converted to strings. We
         * will remove entries from this temporary map as they are emitted under
         * the appropriate column. This is an ordered map so that [showAll] will
         * place any additional columns into an order.
         */
        final Map<String,String> tmp = new TreeMap<String,String>();        
        {
            
            Iterator itr = data.entrySet().iterator();
            
            while(itr.hasNext()) {
                
                Map.Entry entry = (Map.Entry)itr.next();
                
                tmp.put(""+entry.getKey(), ""+entry.getValue());
                
            }
            
        }

        /*
         * Handle the columns that were named by the caller.
         */
        for(int i=0; i<cols.length; i++) {
            
            String col = cols[i];
            
            String val = tmp.remove(col); // remove as we go.

            if (val != null)
                sb.append(val); // iff value defined for that col.
            
            sb.append("\t"); // tab delimited.

        }

        /*
         * Process any remaining columns.
         */
        if(showAll) {
        
            Iterator<Map.Entry<String,String>> itr = tmp.entrySet().iterator();
            
            while(itr.hasNext()) {
                
                Map.Entry<String,String> entry = itr.next();
                
                sb.append(entry.getKey()+"="+entry.getValue()+"\t");
                
            }
            
        }
                    
        return sb.toString();
        
    }
    
    /**
     * An experimental condition.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Condition {

        public final Properties properties;
        
        public Result result;
        
        public Condition(Properties properties) {
            
            this.properties = PropertyUtil.flatCopy(properties);
            
        }
        
        public Condition(Map<String,String> properties) {
            
            this.properties = new Properties();
            
            Iterator<Map.Entry<String,String>> itr = properties.entrySet().iterator();
            
            while(itr.hasNext()) {
                
                Map.Entry<String,String> entry = itr.next();
                
                this.properties.setProperty(entry.getKey(),entry.getValue());
                
            }
            
        }

    }

    /**
     * Accepts a list of conditions and an array of NV[]s and returns a new list
     * of conditions in each original condition has been expanded into N new
     * conditions, one per element of the NV[]s array. This can be used to
     * systematically build up hypercubes in the experimental design that can
     * then be analyzed with a pivot table.
     * 
     * @param conditions
     * @param a
     * @return
     */
    public static List<Condition> apply(List<Condition> conditions, NV[]a) {
        
        List<Condition> ret = new LinkedList<Condition>();
        
        for(int i=0; i<a.length; i++) {
            
            Iterator<Condition> itr = conditions.iterator();
            
            while(itr.hasNext()) {
                
                Condition c = itr.next();
                
                Properties properties = new Properties(c.properties);
                
                properties.put(a[i].getName(),a[i].getValue());
                
                ret.add(new Condition(properties));
                
            }
            
        }
        
        System.err.println("There are now "+ret.size()+" conditions");
        
        return ret;
        
    }
    
    /**
     * Variant that allows multiple factors to vary at a time.
     * @param conditions
     * @param a
     * @return
     */
    public static List<Condition> apply(List<Condition> conditions, NV[][]a) {
        
        List<Condition> ret = new LinkedList<Condition>();
        
        for(int i=0; i<a.length; i++) {
            
            Iterator<Condition> itr = conditions.iterator();
            
            while(itr.hasNext()) {
                
                Condition c = itr.next();
                
                Properties properties = new Properties(c.properties);
        
                for(int j=0; j<a[i].length; j++) {
                
                    properties.put(a[i][j].getName(),a[i][j].getValue());
                    
                }
                
                ret.add(new Condition(properties));
                
            }
            
        }

        System.err.println("There are now "+ret.size()+" conditions");

        return ret;
        
    }
    
    /**
     * Returns a list that contains N copies of the original conditions.
     * 
     * @param nruns
     *            The #of copies to make.
     *            
     * @return The new set of conditions.
     */
    static public Collection<Condition> replicateConditions(int nruns,
            Collection<Condition> conditions) {
        
        List<Condition> ret = new LinkedList<Condition>();
        
        for (Condition c : conditions) {
            
            for(int i=0; i<nruns; i++) {

                ret.add( c );
                
            }
            
        }
        
        return ret;
        
    }

    /**
     * Randomize the conditions.
     * 
     * @param conditions
     * @return
     */
    static public Collection<Condition> randomize(Collection<Condition> conditions) {
        
        Condition[] a = conditions.toArray(new Condition[conditions.size()]);
        
        int[] order = TestCase2.getRandomOrder(a.length);
        
        List<Condition> ret = new ArrayList<Condition>(a.length);
        
        // add to result in permutated order.
        for( int i : order ) {
            
            ret.add(a[i]);
            
        }
        
        return ret;
        
    }
    
    /**
     * Models an experiment.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class Experiment {
        
        public final String className;
        
        public final Map<String,String> defaultProperties;
        
        public final Collection<Condition> _conditions;
        
        public Experiment(String className,
                Map<String, String> defaultProperties,
                Collection<Condition> conditions) {
            
            if (className == null)
                throw new IllegalArgumentException();

            if (defaultProperties == null)
                throw new IllegalArgumentException();

            if (conditions == null)
                throw new IllegalArgumentException();
            
            this.className = className;
            
            this.defaultProperties = defaultProperties;
            
            this._conditions = conditions;
            
        }

        /**
         * Serialize as XML.
         */
        public String toXML() {
            
            StringBuilder sb = new StringBuilder();

            sb.append("<?xml version=\"1.0\" ?>\n");
            
            sb.append("<!DOCTYPE experiment PUBLIC"+
                    " \"" + DTDValidationHelper.PUBLIC_EXPERIMENT_0_1 + "\""+
                    " \"" + DTDValidationHelper.SYSTEM_EXPERIMENT_FILENAME_0_1 + "\""+
                    ">\n");
            
            sb.append("<!-- There are "+_conditions.size()+" conditions. -->\n");
            
            sb.append("<experiment class=\""+className+"\">\n");

            if(!defaultProperties.isEmpty()) {

                sb.append(" <defaults>\n");

                Iterator<Map.Entry<String,String>> itr = defaultProperties.entrySet().iterator();
                
                while(itr.hasNext()) {
                    
                    Map.Entry<String,String> entry = itr.next();
                    
                    sb.append("  <property name=\""+entry.getKey()+"\">"+entry.getValue()+"</property>\n");
                    
                }
                
                sb.append(" </defaults>\n");
                
            }

            Iterator<Condition> conditr = _conditions.iterator();

            int n = 0;
            
            while(conditr.hasNext()) {
                
                Condition cond = conditr.next();
                
                sb.append(" <!-- condition#"+(n+1)+" -->\n");
                
                sb.append(" <condition>\n");

                {
                
                    Iterator itr = cond.properties.entrySet().iterator();

                    while (itr.hasNext()) {

                        Map.Entry entry = (Map.Entry) itr.next();

                        String name = ""+entry.getKey();
                        
                        String value = ""+entry.getValue();
                        
                        if(defaultProperties.get(name)!=null && defaultProperties.get(name).equals(value)) {
                            
                            // Skip defaults that have not been overriden.
                            
                            continue;
                            
                        }
                        
                        sb.append("  <property name=\"" + entry.getKey()
                                + "\">" + entry.getValue() + "</property>\n");

                    }
                
                }

                /*
                 * Write out results when they are available.
                 */
                if (cond.result != null) {
                    
                    Iterator itr = cond.result.entrySet().iterator();

                    while (itr.hasNext()) {

                        Map.Entry entry = (Map.Entry) itr.next();

                        sb.append("  <result name=\"" + entry.getKey()
                                + "\">" + entry.getValue() + "</result>\n");

                    }
                
                }

                sb.append(" </condition>\n");

                n++;
                
            }
            
            sb.append("</experiment>\n");
            
            return sb.toString();
            
        }

        /**
         * Run the experiment, writing the results onto a CSV file.
         * 
         * @param randomize
         *            When true, the {@link Condition}s will be executed in a
         *            random ordering.
         * @param nruns
         *            The #of times to execute all of the {@link Condition}s.
         */
        public void run(boolean randomize,int nruns) throws Exception {
                        
            if (nruns < 1)
                throw new IllegalArgumentException(
                        "nruns must be at least one, not " + nruns);

            // replicate for multiple runs.
            Collection<Condition> conditions = replicateConditions(nruns, _conditions);
            
            if(randomize) {
                
                // randomize across all runs.
                conditions = randomize(conditions);
                
            }
            
            File outFile = new File(className+".exp.csv");

            final boolean exists = outFile.exists();
            
            if (exists) {

                System.err.println("Will append to existing file: "
                        + outFile.getAbsolutePath());

            }

            FileWriter writer = new FileWriter(outFile,true/*append*/);

            final long runStartTime = System.currentTimeMillis();
            
            final int nconditions = conditions.size();
            
            final Properties systemProperties = getInterestingSystemProperties();
            
            try {

                for (int run = 0; run < nruns; run++) {

                    System.err.println("Running comparison of " + nconditions
                            + " conditions for " + className);

                    // show interesting system properties.
                    systemProperties.list(System.err);

                    Class cl = Class.forName(className);

                    Iterator<Condition> itr = conditions.iterator();

                    int i = 0;

                    while (itr.hasNext()) {

                        Condition condition = itr.next();

                        IComparisonTest test = (IComparisonTest) cl
                                .newInstance();

                        System.err.println("Run " + run + " of " + nruns
                                + ", Running condition " + i + " of "
                                + nconditions);

                        try {

                            // setup
                            test.setUpComparisonTest(condition.properties);

                            // run and record the result.
                            condition.result = test
                                    .doComparisonTest(condition.properties);

                        } catch (Throwable t) {

                            log.warn("Error running condition: " + t, t);

                            // record error result.
                            condition.result = Result.errorFactory(t);

                        }

                        try {

                            // tear down.
                            test.tearDownComparisonTest();

                        } catch (Throwable t) {

                            System.err.println("Could not tear down test: "
                                    + t.getMessage());

                        }

                        // add the run date for the condition.
                        condition.properties.setProperty("Date", new Date(
                                System.currentTimeMillis()).toString());

                        // add the run number
                        condition.properties.setProperty("Run", "" + i);

                        // add the sequence number
                        condition.properties.setProperty("Sequence", "" + i);

                        // add the "interesting" system properties.
                        condition.properties.putAll(systemProperties);

                        // the memory currently in use.
                        condition.result.put("java.Runtime.totalMemory", ""
                                + Runtime.getRuntime().totalMemory());

                        // the memory currently in use.
                        condition.result.put("java.Runtime.freeMemory", ""
                                + Runtime.getRuntime().freeMemory());

                        System.err.println(condition.result.toString());

                        i++;

                    }

                } // next run.
            
            }

            catch(Throwable t) {
                
                t.printStackTrace(System.err);
                                
            }
            
            finally {
            
                /*
                 * Always write the summary onto the file.
                 */

                if(exists) {

                    // separate each run in the file with some blank lines.
                    writer.write("\n\n");

                }
                
                writer.write("Run: "+new Date(runStartTime)+"\n\n");

                /*
                 * Collect the distinct condition and result columns.
                 * 
                 * Note: This extracts all columns whose values are constants
                 * for a given run and write them out once at the top of the
                 * run. This simplifies looking for invariants, conditions and
                 * variables in the results.
                 */
                Set<Object> conditionColumns = new TreeSet<Object>();
                Set<Object> resultColumns = new TreeSet<Object>();
                Map<Object,Object> invariants = new HashMap<Object,Object>();
                {

                    boolean first = true;
                    
                    Iterator<Condition> itr = conditions.iterator();

                    while (itr.hasNext()) {

                        Condition condition = itr.next();

                        Map<Object, Object> conditionProperties = PropertyUtil
                                .flatten(condition.properties);
                        
                        // collect unique condition columns.
                        conditionColumns.addAll(conditionProperties.keySet());

                        // collect unique result columns.
                        resultColumns.addAll(condition.result.keySet());

                        if(first) {
                            
                            invariants.putAll(conditionProperties);
                            
                            invariants.putAll(condition.result);
                            
                        } else {
                            
                            // disprove invariants from conditions.
                            {
                                Iterator<Map.Entry<Object,Object>> itr2 = conditionProperties.entrySet().iterator();
                                
                                while(itr2.hasNext()) {
                                    
                                    Map.Entry entry = itr2.next();
                                    
                                    Object key = entry.getKey();
                                    
                                    Object val = entry.getValue();
                                    
                                    if(!val.equals(invariants.get(key))) {
                                        
                                        // proven not to be an invariant.
                                        invariants.remove(key);
                                        
                                    }
                                    
                                }
                                
                            }
                            
                            // disprove invariants from results.
                            {
                                
                                final Iterator<Map.Entry<String, String>> itr2 = condition.result
                                        .entrySet().iterator();
                                
                                while(itr2.hasNext()) {
                                    
                                    final Map.Entry<?,?> entry = itr2.next();
                                    
                                    final Object key = entry.getKey();
                                    
                                    final Object val = entry.getValue();
                                    
                                    if(!val.equals(invariants.get(key))) {
                                        
                                        // proven not to be an invariant.
                                        invariants.remove(key);
                                        
                                    }
                                    
                                }
                                
                            }
                            
                        }
                        
                        first = false;
                        
                    }

                    /*
                     * Remove column headings that were identified as invariants.
                     */
                    {
                        
                        Iterator<Object> itr2 = invariants.keySet().iterator();
                        
                        while(itr2.hasNext()) {
                        
                            Object key = itr2.next();
                            
                            conditionColumns.remove(key);

                            resultColumns.remove(key);
                            
                        }
                        
                    }
                    
                }

                /*
                 * Write out the invariants across the runs.
                 */
                {
                    
                    writer.write("Invariants:\n");

                    Iterator<Map.Entry<Object, Object>> itr = invariants.entrySet().iterator();
                    
                    while(itr.hasNext()) {

                        writer.write(itr.next().getKey()+"\t");
                    
                    }

                    writer.write("\n");
                    
                    itr = invariants.entrySet().iterator();
                    
                    while(itr.hasNext()) {

                        writer.write(invariants.get(itr.next().getKey())+"\t");
                    
                    }

                    writer.write("\n\n");
                    
                }
                
                // write the condition column headings, building cols[] as we go.
                final String[] conditionCols = new String[conditionColumns.size()];
                {
                    
                    Iterator<Object> colitr = conditionColumns.iterator();

                    int i = 0;
                    
                    while(colitr.hasNext()) {
                        
                        String col = ""+colitr.next();
                        
                        writer.write(col+(colitr.hasNext()?"\t":""));
                        
                        conditionCols[i++] = col;
                        
                    }
                    
                }

                // delimiter between the condition col headings and the result col headings.
                writer.write("\t");
                            
                final String[] resultCols = new String[resultColumns.size()];
                {

                    Iterator<Object> colitr = resultColumns.iterator();

                    int i = 0;
                    
                    while(colitr.hasNext()) {
                        
                        String col = ""+colitr.next();
                        
                        writer.write(col+(colitr.hasNext()?"\t":""));
                        
                        resultCols[i++] = col;
                        
                    }

                }
                
                // end of the columns.
                writer.write("\n");

                // write each condition and its outcome (if any).
                {

                    Iterator<Condition> itr = conditions.iterator();

                    while (itr.hasNext()) {

                        Condition condition = itr.next();

                        writer.write(ExperimentDriver.toString(PropertyUtil
                                .flatten(condition.properties), conditionCols,
                                false));

                        writer.write(ExperimentDriver.toString(
                                condition.result, resultCols, false));

                        writer.write("\n");

                    }

                }
                
                writer.flush();
                
                writer.close();
                
            }

            // @todo this is using a different representation from the file which
            // is more difficult to read.
            {
            
                System.err.println("Result summary:");

                Iterator<Condition> itr = conditions.iterator();

                while (itr.hasNext()) {

                    Condition condition = itr.next();

                    System.err.println(condition.result.toString());

                }

            }

        }
        
    }

    /**
     * Helper class for validating {@link Experiment} documents.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static class DTDValidationHelper extends DefaultHandler2 implements EntityResolver2 {

        /**
         * The PUBLIC identifier for the inline-mention-form DTD, version 0.1.
         */
        public static final String PUBLIC_EXPERIMENT_0_1 = "-//systap.com//DTD BIGDATA EXPERIMENT 0.1//EN";

        /**
         * The name of the file containing the DTD (without any path
         * information).
         */
        public static final String SYSTEM_EXPERIMENT_FILENAME_0_1 = "Experiment.dtd";

//        /**
//         * The Java resource containing the inline-mention-form DTD, version
//         * 0.1.
//         */
//        public static final String SYSTEM_EXPERIMENT_RESOURCE_0_1 = "com/bigdata/journal/"
//                + SYSTEM_EXPERIMENT_FILENAME_0_1;    

        /**
         * Validate an inline-mention-form document against a DTD.
         * 
         * @param source
         *            The source from which the document will be read.
         * 
         * @throws SAXException
         *             If validation fails.
         * @throws ParserConfigurationException
         *             If there is a problem configuring the parser.
         * @throws IOException
         *             If there is an IO problem.
         */
        public static void validate(InputSource source) throws SAXException,
                ParserConfigurationException, IOException {

            SAXParserFactory factory = SAXParserFactory.newInstance();

            factory.setValidating(true);
            
            SAXParser parser = factory.newSAXParser();
            
            parser.parse( source, new DTDValidationHelper() );

        }
        
        /**
         * Resolve the inline-mention-form DTD.
         */
        public InputSource resolveEntity(String name, String publicId,
                String baseURI, String systemId) throws SAXException,
                IOException {

            if(INFO) log.info("resolveEntity(name=" + name + ", publicId=" + publicId
                    + ", baseURI=" + baseURI + ", systemId=" + systemId);
            
            if (publicId.equals(PUBLIC_EXPERIMENT_0_1)) {
            
                if(INFO) log.info("Resolving DTD using PUBLIC identifier: " + publicId);
                
                InputStream is = getClass().getResourceAsStream(
                        SYSTEM_EXPERIMENT_FILENAME_0_1);
                
                if (is == null) {
                
                    throw new AssertionError("Could not locate resource: "
                            + SYSTEM_EXPERIMENT_FILENAME_0_1);
                    
                
                }
                
                return new InputSource(is);
                
            } else {
                
                // use the default behaviour
                
                if(INFO) log.info("Using default resolver behavior.");
                
                return super.resolveEntity(name, publicId, baseURI, systemId);
                
            }
            
        }

        /**
         * Log the warning.
         */
        public void warning(SAXParseException e) throws SAXException {

            log.warn(e);
            
        }

        /**
         * Re-throws exception in order to cause a validation error to halt
         * processing (the default behavior ignores errors).
         */
        public void error(SAXParseException e) throws SAXException {
            
            throw e;
            
        }

    }

    /**
     * Parses an XML representation of an {@link Experiment}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class DTDParserHelper extends DTDValidationHelper {
        
        protected String className;

        protected Map<String,String> defaultProperties = null;
        
        protected Collection<Condition> conditions = new Vector<Condition>();
        
        public Experiment parse(InputSource source) throws SAXException, IOException, ParserConfigurationException {

            SAXParserFactory factory = SAXParserFactory.newInstance();
            
            factory.setValidating(true);
            
            SAXParser parser = factory.newSAXParser();
            
            parser.parse( source, this );

            return new Experiment(className,defaultProperties,conditions);
            
        }

        /*
         * used to track partial state during the parse.
         */

        private boolean inDefaults = false;
        private boolean inCondition = false;
        private Map<String,String> properties = null;
        private String pname = null;
        private StringBuilder pvalue = new StringBuilder();
        
        public void startElement(String uri, String localName, String qName,
                Attributes atts) throws SAXException {

            if( qName.equals("experiment")) {
                
                className = atts.getValue("class");
                
            } else if (qName.equals("defaults")) {

                inDefaults = true;
                
                properties = new HashMap<String,String>();

            } else if( qName.equals("condition")) {

                inCondition = true;
                
                properties = new HashMap<String,String>();

            } else if( qName.equals("property")) {
                
                pname = atts.getValue("name");
                
            }
            
        }

        public void endElement(String uri, String localName, String qName)
                throws SAXException {

            if (qName.equals("defaults")) {

                inDefaults = false;
                
                defaultProperties = properties;
                
                properties = null;
                
            } else if (qName.equals("condition")) {

                inCondition = false;

                Map<String,String> tmp = new HashMap<String,String>();
                
                tmp.putAll(defaultProperties);
                
                tmp.putAll(properties);
                
                Condition condition = new Condition(tmp);

                conditions.add(condition);
                
                properties = null;
                
            } else if( qName.equals("property")) { 
            
                properties.put(pname,pvalue.toString());

                // clear the buffer.
                pvalue.setLength(0);
                
            }

        }
        
        /**
         * Collect the property value.
         */
        public void characters(char[] ch, int start, int length)
                throws SAXException {

            pvalue.append(ch, start, length);

        }

        public void endDocument() throws SAXException {

            super.endDocument();

            if (className == null)
                throw new SAXException("'class' attribute not recovered.");

            if (defaultProperties == null)
                throw new SAXException("defaults not recovered.");

            if (conditions.size() == 0) {
                
                log.warn("There are no conditions in this experiment");
                
            }
            
        }

    }

    /**
     * Return a {@link Properties} object that inherits defaults from
     * <i>properties</i> and sets/overrides properties identified in <i>entries</i>.
     * 
     * @param properties
     *            The inherited properties (this object is NOT modified).
     * @param params
     *            The overriden properties.
     *            
     * @return A new {@link Properties}.
     */
    protected static Condition getCondition(Map<String,String>properties, NV[] params) throws IOException {

        properties = new HashMap<String,String>(properties);

        for(int i=0; i<params.length; i++) {
            
            properties.put(params[i].getName(),params[i].getValue());
            
        }

        return new Condition(properties);
        
    }

    /**
     * Runs a comparison of various an {@link IComparisonTest} under various
     * conditions and writes a CSV representation of the reported results.
     * 
     * @param args
     *            The name of the XML file describing the experiment to be run.
     */
    public static void main(final String[] args) throws Exception {

        if (args.length == 0) {

            System.err.println("usage: <experiment.xml> (#runs? (randomize?))");

            System.exit(1);

        }

        int nruns = 1;

        if (args.length == 2) {

            nruns = Integer.parseInt(args[1]);

        }

        boolean randomize = true;

        if (args.length == 3) {

            randomize = Boolean.parseBoolean(args[2]);

        }

        doMain(new File(args[0]), nruns, randomize);

    }

    /**
     * Run an experiment.
     * 
     * @param file
     *            The file describing the experiment.
     * @param nruns
     *            The #of runs.
     * @param randomize
     *            <code>true</code> if the run order should be randomized.
     *            
     * @throws Exception
     */
    public static void doMain(File file, int nruns, boolean randomize)
            throws Exception {

        final Experiment exp = new DTDParserHelper().parse(new InputSource(
                new FileReader(file)));

        exp.run(randomize, nruns);
        
    }

    /**
     * Filter the system properties to report only those that are interesting
     * summaries of the execution environment.
     */
    static public Properties getInterestingSystemProperties() {

        Properties props = new Properties();
        
        {
            Iterator itr = PropertyUtil.flatten(System.getProperties()).entrySet().iterator();
            
            while( itr.hasNext() ) {
                Map.Entry entry = (Map.Entry) itr.next();
                String pname = (String)entry.getKey();
                String pvalue = (String) entry.getValue();
                if( pname.startsWith("line.")) continue;
                if( pname.startsWith("path.")) continue;
                if( pname.startsWith("os.")) continue;
                if( pname.startsWith("java.") && !(
                        pname.equals("java.vm.vendor") ||
                        pname.equals("java.vm.version") ||
                        pname.equals("java.vm.name")
                        )) continue;
                if( pname.startsWith("sun.")) continue;
                if( pname.startsWith("file.")) continue;
                if( pname.startsWith("user.")) continue;
                if( pname.startsWith("maven.")) continue;
                if( pname.startsWith("awt.")) continue;
                if( pname.startsWith("junit.")) continue;
                if( pname.startsWith("cactus.")) continue;
                if( pname.startsWith("tag")) continue; // tag1, tag2, etc. (maven).
                props.setProperty(pname, pvalue);
                itr.remove();
    
            }
        }
        
        // report unused properties.
        {
            
            Iterator itr = PropertyUtil.flatten(System.getProperties()).entrySet().iterator();
            
            while(itr.hasNext()) {
                
                Map.Entry entry = (Map.Entry) itr.next();
                String pname = (String)entry.getKey();
                String pvalue = (String) entry.getValue();
                
                System.err.println("Property not reported: "+pname+"="+pvalue);
                
            }
            
        }
   
        /*
         * Set some other properties that are interesting.
         */

        // maximum memory the system will attempt to use.
        props.setProperty("java.Runtime.maxMemory", ""+Runtime.getRuntime().maxMemory());

        props.setProperty("os.arch.cpus", ""+SystemUtil.numProcessors());
        
        try {
            
            props.setProperty("host",InetAddress.getLocalHost().getHostName());
            
        } catch(UnknownHostException ex) {
            /*
             * ignore.
             */
        }
        
        return props;
        
    }
    
}
