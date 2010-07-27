/*
 * Copyright (C) 2008 Andreas Schultz
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package benchmark.testdriver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;

import java.util.Locale;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.*;
import java.util.StringTokenizer;

import benchmark.qualification.QueryResult;

public class TestDriver {
	protected QueryMix queryMix;//The Benchmark Querymix
	protected int warmups = TestDriverDefaultValues.warmups;//how many Query mixes are run for warm up
	protected AbstractParameterPool parameterPool;//Where to get the query parameters from
	protected ServerConnection server;//only important for single threaded runs
	protected String queryMixFN = "querymix.txt";//"qm.txt";
	protected String ignoreQueryFN = "ignoreQueries.txt";//contains Queries to ignore
	protected File queryDir = TestDriverDefaultValues.queryDir;//where to take the queries from
	protected int nrRuns = TestDriverDefaultValues.nrRuns;
	protected long seed = TestDriverDefaultValues.seed;//For the random number generators
	protected String sparqlEndpoint = null;
	protected String defaultGraph = TestDriverDefaultValues.defaultGraph;
	protected String resourceDir = TestDriverDefaultValues.resourceDir;//Where to take the Test Driver data from
	protected String xmlResultFile = TestDriverDefaultValues.xmlResultFile;
	protected static Logger logger = Logger.getLogger( TestDriver.class );
	protected boolean[] ignoreQueries;//Queries to ignore
	protected boolean doSQL = false;
	protected boolean multithreading=false;
	protected int nrThreads;
	protected int timeout = TestDriverDefaultValues.timeoutInMs;
	protected String driverClassName = TestDriverDefaultValues.driverClassName;
	protected boolean qualification = TestDriverDefaultValues.qualification;
	protected String qualificationFile = TestDriverDefaultValues.qualificationFile;
	
	/*
	 * Parameters for steady state
	 */
	protected int qmsPerPeriod = TestDriverDefaultValues.qmsPerPeriod;//Querymixes per measuring period
	protected double percentDifference = TestDriverDefaultValues.percentDifference;//Difference in percent between min and max measurement period
	protected int nrOfPeriods = TestDriverDefaultValues.nrOfPeriods;//The last nrOfPeriods periods are compared
	protected boolean rampup = false;
	
	
	public TestDriver(String[] args) {
		processProgramParameters(args);
		System.out.print("Reading Test Driver data...");
		System.out.flush();
		if(doSQL)
			parameterPool = new SQLParameterPool(new File(resourceDir),seed);
		else
			parameterPool = new LocalSPARQLParameterPool(new File(resourceDir),seed);
		System.out.println("done");
	
		if(sparqlEndpoint!=null && !multithreading){
			if(doSQL)
				server = new SQLConnection(sparqlEndpoint, timeout, driverClassName);
			else
				server = new SPARQLConnection(sparqlEndpoint, defaultGraph, timeout);
		} else if(multithreading) {
			//do nothing
		}
		else {
			printUsageInfos();
			System.exit(-1);
		}
		
		TestDriverShutdown tds = new TestDriverShutdown(this);
		Runtime.getRuntime().addShutdownHook(tds);
	}
	
	public void init() {
		Query[] queries = null;
		Integer[] queryRun = null;
		
		if(queryMixFN==null) {
			Integer[] temp = { 1, 2, 2, 3, 2, 2, 4, 2, 2, 5, 7, 7, 6, 7, 7, 8, 9, 9, 8, 9, 9, 10, 10, 11, 12};
//			Integer[] temp = { 1, 2, 2, 3, 2, 2, 4, 2, 2, 5, 7, 7, 6, 7, 7, 8, 9, 9, 8, 9, 9, 10, 10, 10, 10};
//			Integer[] temp = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
//			Integer[] temp = { 8 };

			queryRun = temp;
		}
		else {
			queryRun = getQueryMixInfo(queryMixFN);
		}
		
		Integer maxQueryNr = 0;
		for(int i=0;i<queryRun.length;i++) {
			if(queryRun[i] != null && queryRun[i] > maxQueryNr)
				maxQueryNr = queryRun[i];
		}

		queries = new Query[maxQueryNr];
		
		if(ignoreQueryFN==null) {
			ignoreQueries = new boolean[maxQueryNr];
			
			for(int i=0;i<ignoreQueries.length;i++) {
				ignoreQueries[i] = false;
			}
			
			//WHICH QUERIES TO IGNORE
//			ignoreQueries[4] = true;
		}
		else
			ignoreQueries = getIgnoreQueryInfo(ignoreQueryFN, maxQueryNr);
		
		for(int i=0;i<queries.length;i++) {
			queries[i] = null;
		}
		
		for(int i=0;i<queryRun.length;i++) {
			if(queryRun[i]!=null) {
				Integer qnr = queryRun[i];
				if(queries[qnr-1]==null) {
					File queryFile = new File(queryDir, "query" + qnr + ".txt");
					File queryDescFile = new File(queryDir, "query" + qnr + "desc.txt");
					if(doSQL)
						queries[qnr-1] = new Query(queryFile, queryDescFile, "@");
					else
						queries[qnr-1] = new Query(queryFile, queryDescFile, "%");
					
					//Read qualification information
					if(qualification) {
						File queryValidFile = new File(queryDir, "query" + qnr + "valid.txt");
						String[] rowNames = getRowNames(queryValidFile);
						queries[qnr-1].setRowNames(rowNames);
					}
				}
			}
		}
		
		
		queryMix = new QueryMix(queries, queryRun);
	}
	
	private String[] getRowNames(File file) {
		ArrayList<String> rowNames = new ArrayList<String>();

		try {
			BufferedReader rowReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

			StringBuffer data = new StringBuffer();
			String line = null;
			while((line=rowReader.readLine())!=null) {
				if(!line.equals("")){
					data.append(line);
					data.append(" ");
				}
			}
				
			StringTokenizer st = new StringTokenizer(data.toString());
			while(st.hasMoreTokens()) {
				rowNames.add(st.nextToken());
			}

		} catch(IOException e) {
			System.err.println("Error processing query qualification-info file: " + file.getAbsolutePath());
			System.exit(-1);
		}
		return rowNames.toArray(new String[1]);
	}
	
	private Integer[] getQueryMixInfo(String queryMixFilename) {
		System.out.println("Reading query mix file: " + queryMixFilename);
		File file = new File(queryMixFilename);
		ArrayList<Integer> qm = new ArrayList<Integer>();

		try {
			BufferedReader qmReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

			StringBuffer data = new StringBuffer();
			String line = null;
			while((line=qmReader.readLine())!=null) {
				if(!line.equals("")){
					data.append(line);
					data.append(" ");
				}
			}
				
			StringTokenizer st = new StringTokenizer(data.toString());
			while(st.hasMoreTokens()) {
				qm.add(Integer.parseInt(st.nextToken()));
			}

		} catch(IOException e) {
			System.err.println("Error processing query mix file: " + queryMixFilename);
			System.exit(-1);
		}
		return qm.toArray(new Integer[1]);
	}
	
	private boolean[] getIgnoreQueryInfo(String ignoreListFilename, int maxQueryNumber) {
		System.out.println("Reading query ignore file: " + ignoreListFilename);
		File file = new File(ignoreListFilename);
		boolean[] ignoreQueries = new boolean[maxQueryNumber];
		
		for(int i=0;i<maxQueryNumber;i++)
			ignoreQueries[i] = false;
		
		try {
			BufferedReader qmReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));

			StringBuffer data = new StringBuffer();
			String line = null;
			while((line=qmReader.readLine())!=null) {
				if(!line.equals("")){
					data.append(line);
					data.append(" ");
				}
			}
				
			StringTokenizer st = new StringTokenizer(data.toString());
			while(st.hasMoreTokens()) {
				Integer queryNr = Integer.parseInt(st.nextToken());
				if(queryNr>0 && queryNr<=maxQueryNumber)
					ignoreQueries[queryNr-1] = true;
			}

		} catch(IOException e) {
			System.err.println("Error processing query ignore file: " + ignoreListFilename);
			System.exit(-1);
		}
		return ignoreQueries;
	}
	
	public void run() {
		int qmsPerPeriod = TestDriverDefaultValues.qmsPerPeriod;
		int qmsCounter = 0;
		int periodCounter = 0;
		double periodRuntime = 0;
		BufferedWriter measurementFile = null;
		try{
			measurementFile = new BufferedWriter(new FileWriter("steadystate.tsv"));
		} catch(IOException e) { System.err.println("Could not create file steadystae.tsv!"); System.exit(-1);}
		
		for(int nrRun=-warmups;nrRun<nrRuns;nrRun++) {
			long startTime = System.currentTimeMillis();
			queryMix.setRun(nrRun);
			while(queryMix.hasNext()) {
				Query next = queryMix.getNext();
				Object[] queryParameters = parameterPool.getParametersForQuery(next);
				next.setParameters(queryParameters);
				if(ignoreQueries[next.getNr()-1])
					queryMix.setCurrent(0, -1.0);
				else {
					server.executeQuery(next, next.getQueryType());
				}
			}
			
			//Ignore warm-up measures
			if(nrRun>=0) {
				qmsCounter++;
				periodRuntime += queryMix.getQueryMixRuntime();
			}
			
			//Write out period data
			if(qmsCounter==qmsPerPeriod) {
				periodCounter++;
				try{
					measurementFile.append(periodCounter + "\t" + periodRuntime + "\n");
					measurementFile.flush();
				} catch(IOException e) { e.printStackTrace(); System.exit(-1);}
				periodRuntime = 0;
				qmsCounter = 0;
			}
			
			System.out.println(nrRun + ": " + String.format(Locale.US, "%.2f", queryMix.getQueryMixRuntime()*1000)
					+ "ms, total: " + (System.currentTimeMillis()-startTime) + "ms");
			queryMix.finishRun();
		}
		logger.log(Level.ALL, printResults(true));
		try {
			FileWriter resultWriter = new FileWriter(xmlResultFile);
			resultWriter.append(printXMLResults(true));
			resultWriter.flush();
			resultWriter.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void runQualification() {
		File file = new File(qualificationFile);
		System.out.println("Creating qualification file: " + file.getAbsolutePath() + "\n");
		try{
			file.createNewFile();
			ObjectOutputStream objectOutput = new ObjectOutputStream(new FileOutputStream(file, false));
			objectOutput.writeInt(queryMix.getQueries().length);
			objectOutput.writeLong(seed);
			objectOutput.writeInt(parameterPool.getScalefactor());
			objectOutput.writeInt(nrRuns);
			objectOutput.writeObject(queryMix.getQueryMix());
			objectOutput.writeObject(ignoreQueries);
			
			for(int nrRun=0;nrRun<nrRuns;nrRun++) {
				queryMix.setRun(nrRun+1);
				System.out.print("Run: " + (nrRun + 1));
				while(queryMix.hasNext()) {
					Query next = queryMix.getNext();
					Object[] queryParameters = parameterPool.getParametersForQuery(next);
					next.setParameters(queryParameters);
					if(ignoreQueries[next.getNr()-1])
						queryMix.setCurrent(0, -1.0);
					else {
						QueryResult queryResult = server.executeValidation(next, next.getQueryType());
						if(queryResult!=null)
							objectOutput.writeObject(queryResult);
						queryMix.setCurrent(0, -1.0);
					}
					System.out.print(".");
				}
				queryMix.finishRun();
				System.out.println("done");
			}
			objectOutput.flush();
			objectOutput.close();
			System.out.println("\nQualification file created!");
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/*
	 * Ramp-up: Runs at least nrOfPeriods periods and checks after every period if the last nrOfPeriods periods
	 * differ at most percentDifference from each other. There is also a check if one of the examined periods
	 * is the minimum overall period, then the percentage check will be ignored.
	 *  
	 */
	public void runRampup() {
		System.out.println("Starting Ramp-up. Writing measurement data to rampup.tsv");
		BufferedWriter measurementFile = null;
		try{
			measurementFile = new BufferedWriter(new FileWriter("rampup.tsv"));
		} catch(IOException e) { System.err.println("Could not create file rampup.tsv!"); System.exit(-1);}
	
		int periodNr = 0;
		LinkedList<Double> periods = new LinkedList<Double>();
		double totalRuntime = 0;
		boolean unsteady = true;//Set to false after reaching steady state
		double minimumPeriod = Double.MAX_VALUE;
		while(unsteady) {
			
			periodNr++;
			double runtime = 0;
			//Running one period
			for(int nrRun=1;nrRun<=qmsPerPeriod;nrRun++) {
				queryMix.setRun(nrRun);
				while(queryMix.hasNext()) {
					Query next = queryMix.getNext();
					Object[] queryParameters = parameterPool.getParametersForQuery(next);
					next.setParameters(queryParameters);
					if(ignoreQueries[next.getNr()-1])
						queryMix.setCurrent(0, -1.0);
					else {
						server.executeQuery(next, next.getQueryType());
					}
				}
				runtime += queryMix.getQueryMixRuntime();
				System.out.println("Period " + periodNr + " Run: " + nrRun + ": " + String.format(Locale.US, "%.3f", queryMix.getQueryMixRuntime()*1000)+ "ms");
				queryMix.finishRun();
			}
			
			//Write period/runtime pairs into rampup.tsv
			try{
				measurementFile.append(periodNr + "\t" + runtime + "\n");
				measurementFile.flush();
				} catch(IOException e) { e.printStackTrace(); System.exit(-1);}
			totalRuntime += runtime;
			
			if(periodNr<=nrOfPeriods) {
				periods.addLast(runtime);
			} else {
				periods.addLast(runtime);
				periods.removeFirst();
				if(periods.size()!=nrOfPeriods)
					throw new AssertionError();
				
				//Calculate difference between the periods
				double min = Double.MAX_VALUE;
				double max = Double.MIN_VALUE;
				for(double pVal: periods) {
					if(pVal < min)
						min = pVal;
					if(pVal > max)
						max = pVal;
					if(pVal <= minimumPeriod) {
						minimumPeriod = pVal;
						max = Double.MAX_VALUE;
						break;//Special case: If this period is lower than the previous minimum period, go on.
					}
				}
				if((max-min)/min < percentDifference)
					unsteady = false;
			}
			
			double all5 = 0;
			for(double pVal: periods) {
				all5 += pVal;
			}
			System.out.println("Total execution time for period " + periodNr + "/last " + (periodNr<nrOfPeriods?periodNr:nrOfPeriods) + " periods: " + String.format(Locale.US, "%.3f",runtime*1000) + "ms/"+ String.format(Locale.US, "%.3f",all5*1000) +"ms\n");
		}
		System.out.println("Steady state reached after " + periodNr + " measurement periods/" + String.format(Locale.US, "%.3f",totalRuntime) + "s");
		server.close();
	}
	
	/*
	 * run the test driver in multi-threaded mode
	 */
	public void runMT() {
		ClientManager manager = new ClientManager(parameterPool, this);
		
		manager.createClients();
		manager.startWarmup();
		manager.startRun();
		logger.log(Level.ALL, printResults(true));
		try {
			FileWriter resultWriter = new FileWriter(xmlResultFile);
			resultWriter.append(printXMLResults(true));
			resultWriter.flush();
			resultWriter.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Process the program parameters typed on the command line.
	 */
	protected void processProgramParameters(String[] args) {
		int i=0;
		while(i<args.length) {
			try {
				if(args[i].equals("-runs")) {
					nrRuns = Integer.parseInt(args[i++ + 1]);
				}
				else if(args[i].equals("-idir")) {
					resourceDir = args[i++ + 1];
				}
				else if(args[i].equals("-qdir")) {
					queryDir = new File(args[i++ + 1]);
				}
				else if(args[i].equals("-w")) {
					warmups = Integer.parseInt(args[i++ + 1]);
				}
				else if(args[i].equals("-o")) {
					xmlResultFile = args[i++ + 1];
				}
				else if(args[i].equals("-dg")) {
					defaultGraph = args[i++ + 1];
				}
				else if(args[i].equals("-sql")) {
					doSQL = true;
				}
				else if(args[i].equals("-mt")) {
					if(rampup)
						throw new Exception("Incompatible options: -mt and -rampup");
					multithreading = true;
					nrThreads = Integer.parseInt(args[i++ + 1]);
				}
				else if(args[i].equals("-seed")) {
					final String s = args[i++ + 1];
					if (s.equals("random")) {
						seed = System.nanoTime();
					} else {
						seed = Long.parseLong(s);
					}
				}
				else if(args[i].equals("-t")) {
					timeout = Integer.parseInt(args[i++ + 1]);
				}
				else if (args[i].equals("-dbdriver")) {
					driverClassName = args[i++ + 1];
				}
				else if (args[i].equals("-qf")) {
					qualificationFile = args[i++ + 1];
				}
				else if (args[i].equals("-q")) {
					qualification = true;
					nrRuns = 15;
				}
				else if(args[i].equals("-rampup")) {
					if(multithreading)
						throw new Exception("Incompatible options: -mt and -rampup");
					rampup = true;
					
				}
				else if(!args[i].startsWith("-")) {
					sparqlEndpoint = args[i];
				}
				else {
					System.err.println("Unknown parameter: " + args[i]);
					printUsageInfos();
					System.exit(-1);
				}
				
				i++;
							
			} catch(Exception e) {
				System.err.println("Invalid arguments\n");
				printUsageInfos();
				System.exit(-1);
			}
		}
	}
	
	/*
	 * Get Result String
	 */
	public String printResults(boolean all) {
		StringBuffer sb = new StringBuffer(100);
		double singleMultiRatio = 0.0;
		
		sb.append("Scale factor:           " + parameterPool.getScalefactor() + "\n");
		sb.append("Number of warmup runs:  " + warmups + "\n");
		if(multithreading)
			sb.append("Number of clients:      " + nrThreads + "\n");
		sb.append("Seed:                   " + seed + "\n");
		sb.append("Number of query mix runs (without warmups): " + queryMix.getQueryMixRuns() + " times\n");
		sb.append("min/max Querymix runtime: " + String.format(Locale.US, "%.4fs",queryMix.getMinQueryMixRuntime()) +
				  " / " + String.format(Locale.US, "%.4fs",queryMix.getMaxQueryMixRuntime()) + "\n");
		if(multithreading) {
			sb.append("Total runtime (sum):    " + String.format(Locale.US, "%.3f",queryMix.getTotalRuntime()) + " seconds\n");
			sb.append("Total actual runtime:   " + String.format(Locale.US, "%.3f",queryMix.getMultiThreadRuntime()) + " seconds\n");
			singleMultiRatio = queryMix.getTotalRuntime()/queryMix.getMultiThreadRuntime();
		}
		else
			sb.append("Total runtime:          " + String.format(Locale.US, "%.3f",queryMix.getTotalRuntime()) + " seconds\n");
		if(multithreading)
			sb.append("QMpH:                   " + String.format(Locale.US, "%.2f",queryMix.getMultiThreadQmpH()) + " query mixes per hour\n");
		else
			sb.append("QMpH:                   " + String.format(Locale.US, "%.2f",queryMix.getQmph()) + " query mixes per hour\n");
		sb.append("CQET:                   " + String.format(Locale.US, "%.5f",queryMix.getCQET()) + " seconds average runtime of query mix\n");
		sb.append("CQET (geom.):           " + String.format(Locale.US, "%.5f",queryMix.getQueryMixGeometricMean()) + " seconds geometric mean runtime of query mix\n");
		
		if(all) {
			sb.append("\n");
			//Print per query statistics
			Query[] queries = queryMix.getQueries();
			double[] qmin = queryMix.getQmin();
			double[] qmax = queryMix.getQmax();
			double[] qavga = queryMix.getAqet();//Arithmetic mean
			double[] avgResults = queryMix.getAvgResults();
			double[] qavgg = queryMix.getGeoMean();
			int[] qTimeout = queryMix.getTimeoutsPerQuery();
			int[] minResults = queryMix.getMinResults();
			int[] maxResults = queryMix.getMaxResults();
			int[] nrq = queryMix.getRunsPerQuery();
			for(int i=0;i<qmin.length;i++) {
				if(queries[i]!=null) {
					sb.append("Metrics for Query:      " + (i+1) + "\n");
					sb.append("Count:                  " + nrq[i] + " times executed in whole run\n");
					sb.append("AQET:                   " + String.format(Locale.US, "%.6f",qavga[i]) + " seconds (arithmetic mean)\n");
					sb.append("AQET(geom.):            " + String.format(Locale.US, "%.6f",qavgg[i]) + " seconds (geometric mean)\n");
					if(multithreading)
						sb.append("QPS:                    " + String.format(Locale.US, "%.2f",singleMultiRatio/qavga[i]) + " Queries per second\n");
					else
						sb.append("QPS:                    " + String.format(Locale.US, "%.2f",1/qavga[i]) + " Queries per second\n");
					sb.append("minQET/maxQET:          " + String.format(Locale.US, "%.8fs",qmin[i]) + " / " + 
							String.format(Locale.US, "%.8fs",qmax[i]) + "\n");
					if(queries[i].getQueryType()==Query.SELECT_TYPE) {
						sb.append("Average result count:   " + String.format(Locale.US, "%.2f",avgResults[i]) + "\n");
						sb.append("min/max result count:   " + minResults[i] + " / " + maxResults[i] + "\n");
					} else {
						sb.append("Average result (Bytes): " + String.format(Locale.US, "%.2f",avgResults[i]) + "\n");
						sb.append("min/max result (Bytes): " + minResults[i] + " / " + maxResults[i] + "\n");
					}
					sb.append("Number of timeouts:     " + qTimeout[i] + "\n\n");
				}
			}
		}
		
		return sb.toString();
	}
	
	/*
	 * Get XML Result String
	 */
	public String printXMLResults(boolean all) {
		StringBuffer sb = new StringBuffer(100);
		double singleMultiRatio = 0.0;
		
		sb.append("<?xml version=\"1.0\"?>");
		sb.append("<bsbm>\n");
		sb.append("  <querymix>\n");
		sb.append("     <scalefactor>" + parameterPool.getScalefactor() + "</scalefactor>\n");
		sb.append("     <warmups>" + warmups + "</warmups>\n");
		if(multithreading)
			sb.append("     <nrthreads>" + nrThreads + "</nrthreads>\n");
		sb.append("     <seed>" + seed + "</seed>\n");
		sb.append("     <querymixruns>" + queryMix.getQueryMixRuns() + "</querymixruns>\n");
		sb.append("     <minquerymixruntime>" + 
				  String.format(Locale.US, "%.4f",queryMix.getMinQueryMixRuntime()) + "</minquerymixruntime>\n");
		sb.append("     <maxquerymixruntime>" + 
				  String.format(Locale.US, "%.4f",queryMix.getMaxQueryMixRuntime()) + "</maxquerymixruntime>\n");
		if(multithreading) {
			sb.append("     <totalruntime>" + String.format(Locale.US, "%.3f",queryMix.getTotalRuntime()) + "</totalruntime>\n");
			sb.append("     <actualtotalruntime>" + String.format(Locale.US, "%.3f",queryMix.getMultiThreadRuntime()) + "</actualtotalruntime>\n");
			singleMultiRatio = queryMix.getTotalRuntime()/queryMix.getMultiThreadRuntime();
		}
		else {
			sb.append("     <totalruntime>" + String.format(Locale.US, "%.3f",queryMix.getTotalRuntime()) + "</totalruntime>\n");
			singleMultiRatio = 1;
		}
		
		if(multithreading)
			sb.append("     <qmph>" + String.format(Locale.US, "%.2f",queryMix.getMultiThreadQmpH()) + "</qmph>\n");
		else
			sb.append("     <qmph>" + String.format(Locale.US, "%.2f",queryMix.getQmph()) + "</qmph>\n");
		sb.append("     <cqet>" + String.format(Locale.US, "%.5f",queryMix.getCQET()) + "</cqet>\n");
		sb.append("     <cqetg>" + String.format(Locale.US, "%.5f",queryMix.getQueryMixGeometricMean()) + "</cqetg>\n");
		sb.append("  </querymix>\n");
		
		if(all) {
			sb.append("  <queries>\n");
			//Print per query statistics
			Query[] queries = queryMix.getQueries();
			double[] qmin = queryMix.getQmin();
			double[] qmax = queryMix.getQmax();
			double[] qavga = queryMix.getAqet();
			double[] avgResults = queryMix.getAvgResults();
			double[] qavgg = queryMix.getGeoMean();
			int[] qTimeout = queryMix.getTimeoutsPerQuery();
			int[] minResults = queryMix.getMinResults();
			int[] maxResults = queryMix.getMaxResults();
			int[] nrq = queryMix.getRunsPerQuery();
			for(int i=0;i<qmin.length;i++) {
				if(queries[i]!=null) {
					sb.append("    <query nr=\"" + (i+1) + "\">\n");
					sb.append("      <executecount>" + nrq[i] + "</executecount>\n");
					sb.append("      <aqet>" + String.format(Locale.US, "%.6f",qavga[i]) + "</aqet>\n");
					sb.append("      <aqetg>" + String.format(Locale.US, "%.6f",qavgg[i]) + "</aqetg>\n");
					sb.append("      <qps>" + String.format(Locale.US, "%.2f",singleMultiRatio/qavga[i]) + "</qps>\n");
					sb.append("      <minqet>" + String.format(Locale.US, "%.8f",qmin[i]) + "</minqet>\n");
					sb.append("      <maxqet>" + String.format(Locale.US, "%.8f",qmax[i]) + "</maxqet>\n");
					sb.append("      <avgresults>" + String.format(Locale.US, "%.2f",avgResults[i]) + "</avgresults>\n");
					sb.append("      <minresults>" + minResults[i] + "</minresults>\n");
					sb.append("      <maxresults>" + maxResults[i] + "</maxresults>\n");
					sb.append("      <timeoutcount>" + qTimeout[i] + "</timeoutcount>\n");
					sb.append("    </query>\n");
				}
				else {
					sb.append("    <query nr=\"" + (i+1) + "\">\n");
					sb.append("      <executecount>0</executecount>\n");
					sb.append("      <aqet>0.0</aqet>\n");
					sb.append("    </query>\n");
				}
			}
			sb.append("  </queries>\n");
		}
		sb.append("</bsbm>\n");
		return sb.toString();
	}
	
	/*
	 * print command line options
	 */
	protected void printUsageInfos() {
		String output = "Usage: java benchmark.testdriver.TestDriver <options> SPARQL-Endpoint\n\n" +
						"SPARQL-Endpoint: The URL of the HTTP SPARQL Endpoint\n\n" +
						"Possible options are:\n" +
						"\t-runs <number of query mix runs>\n" +
						"\t\tdefault: " + TestDriverDefaultValues.nrRuns + "\n" +
						"\t-idir <data input directory>\n" +
						"\t\tThe input directory for the Test Driver data\n" +
						"\t\tdefault: " + TestDriverDefaultValues.resourceDir + "\n" +
						"\t-qdir <query directory>\n" +
						"\t\tThe directory containing the query data\n" +
						"\t\tdefault: " + TestDriverDefaultValues.queryDir.getName() + "\n" +
						"\t-w <number of warm up runs before actual measuring>\n" +
						"\t\tdefault: " + TestDriverDefaultValues.warmups + "\n"+
						"\t-o <benchmark results output file>\n" +
						"\t\tdefault: " + TestDriverDefaultValues.xmlResultFile + "\n" +
						"\t-dg <Default Graph>\n" +
						"\t\tdefault: " + TestDriverDefaultValues.defaultGraph + "\n" +
						"\t-sql\n" +
						"\t\tuse JDBC connection to a RDBMS. Instead of a SPARQL-Endpoint, a JDBC URL has to be supplied.\n" +
						"\t\tdefault: not set\n" +
						"\t-mt <Number of clients>\n" +
						"\t\tRun multiple clients concurrently.\n" +
						"\t\tdefault: not set\n" +
						"\t-seed <Long Integer>\n" +
						"\t\tInit the Test Driver with another seed than the default.\n" +
						"\t\tdefault: " + TestDriverDefaultValues.seed + "\n" +
						"\t-t <timeout in ms>\n" +
						"\t\tTimeouts will be logged for the result report.\n" +
						"\t\tdefault: " + TestDriverDefaultValues.timeoutInMs + "ms\n" + 
						"\t-dbdriver <DB-Driver Class Name>\n" +
						"\t\tdefault: " + TestDriverDefaultValues.driverClassName+ "\n" +
						"\t-q\n"  +
						"\t\tTurn on qualification mode instead of doing a test run.\n" +
						"\t\tdefault: " + TestDriverDefaultValues.qualification + "\n" +
						"\t-qf <qualification file name>\n" +
						"\t\tTo change the  filename from its default.\n" +
						"\t\tdefault: " + TestDriverDefaultValues.qualificationFile + "\n" +
						"\t-rampup\n" +
						"\t\tRun ramp-up to reach steady state.\n";
		
		System.out.print(output);
	}
	
	class TestDriverShutdown extends Thread {
		TestDriver testdriver;
		
		TestDriverShutdown(TestDriver t) {
			this.testdriver = t;
		}
		
		public void run() {
			try {
				testdriver.server.close();
			} catch(Exception e) {}
		}
	}
	
	public static void main(String argv[]) {
		DOMConfigurator.configureAndWatch( "log4j.xml", 60*1000 );
		TestDriver testDriver = new TestDriver(argv);
		testDriver.init();
		System.out.println("\nStarting test...\n");
		if(testDriver.multithreading) {
			testDriver.runMT();
			System.out.println("\n" + testDriver.printResults(true));
		}
		else if(testDriver.qualification)
			testDriver.runQualification();
		else if(testDriver.rampup)
			testDriver.runRampup();
		else {
			testDriver.run();
			System.out.println("\n" + testDriver.printResults(true));
		}
	}
}
