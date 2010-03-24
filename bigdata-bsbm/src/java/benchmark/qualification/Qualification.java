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

package benchmark.qualification;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.util.ArrayList;
import java.util.Arrays;

public class Qualification {
	private ObjectInputStream examineStream;
	private ObjectInputStream correctStream;
	private int[] totalQueryCount;
	private int[] correctQueryCount;
	private boolean resultsCountOnly = QualificationDefaultValues.resultsCountOnly;
	private String qualificationLog = QualificationDefaultValues.qualificationLog;
	
	public Qualification(String correctFile, String testFile, String[] args) {
		try {
			examineStream = new ObjectInputStream(new FileInputStream(testFile));
			correctStream = new ObjectInputStream(new FileInputStream(correctFile));
			int maxQuery = Math.max(examineStream.readInt(), correctStream.readInt());
			totalQueryCount = new int[maxQuery];
			correctQueryCount = new int[maxQuery];
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		processProgramParameters(args);
	}
	
	public static void main(String[] argv) {
		if(argv.length<2) {
			printUsageInfo();
			System.exit(-1);
		}
		
		int arglength = argv.length;
		Qualification validator = new Qualification(argv[arglength-2],argv[arglength-1],Arrays.copyOf(argv, arglength-2));
		validator.test();
	}
	
	/*
	 * Process the program option parameters typed on the command line.
	 */
	private void processProgramParameters(String[] args) {
		int i=0;
		while(i<args.length) {
			try {
				if(args[i].equals("-ql")) {
					qualificationLog = args[i++ + 1];
				}
				else if(args[i].equals("-rc")) {
					resultsCountOnly = true;
				}
				else {
					System.err.println("Unknown parameter: " + args[i]);
					printUsageInfo();
					System.exit(-1);
				}
				i++;						
			} catch(Exception e) {
				System.err.println("Invalid arguments\n");
				printUsageInfo();
				System.exit(-1);
			}
		}
	}
	
	private void test() {
		try{
			FileWriter resultWriter = new FileWriter(qualificationLog);
			System.out.println("Starting validation...\n");
			
			//Check seed
			if(examineStream.readLong()!=correctStream.readLong()) {
				System.err.println("Error: Trying to compare runs with different random number generator seeds!");
				System.exit(-1);
			}
			
			//Check scale factor
			if(examineStream.readInt()!=correctStream.readInt()) {
				System.err.println("Error: Trying to compare runs with different scale factors!");
				System.exit(-1);
			}
			
			//Check number of runs
			if(examineStream.readInt()!=correctStream.readInt()) {
				System.err.println("Error: Trying to compare runs with different query mix counts!");
				System.exit(-1);
			}
			
			Integer[] correctQuerymix = (Integer[]) correctStream.readObject();
			Integer[] examineQuerymix = (Integer[]) examineStream.readObject();
			
			//Check ignored queries
			boolean[] correctIgnoreQueries = (boolean[]) correctStream.readObject();
			boolean[] examineIgnoreQueries = (boolean[]) examineStream.readObject();
			
			for(int i=0;i<correctIgnoreQueries.length && i<examineIgnoreQueries.length;i++) {
				if(correctIgnoreQueries[i]!=examineIgnoreQueries[i]) {
					System.err.println("Error: Not the same run setup! Ignored queries (Query " + (i+1) + ") for only one run found.");
					System.exit(-1);
				}
			}
			
			for(int i=0;i<correctQuerymix.length;i++) {
				int a = correctQuerymix[i];
				int b = examineQuerymix[i];
				if(a!=b) {
					System.err.println("Error: Not the same run setup! Querymixes differ from each other at number " + (i+1) + ".");
					System.exit(-1);
				}
			}
			
			String error = null;
			QueryResult examine = null;
			QueryResult correct = null;
			resultWriter.append("Qualification results: Single Queries (the qualification overview is at the end of this file)\n\n");
			//Check single query results
			while(true) {
			  try {
				examine = (QueryResult) examineStream.readObject();
				correct = (QueryResult) correctStream.readObject();
				error = null;
				
				//Check query numbers
				if(examine.getQueryNr()!=correct.getQueryNr()) {
					System.err.println("Error: Query order is different in both runs!");
					System.exit(-1);
				}
				
				totalQueryCount[examine.getQueryNr()-1]++;
				
				//Check variable names of result, only to use for comparing SPARQL results
				if(!resultsCountOnly) {
					ArrayList<String> headExamine = examine.getHeadList();
					ArrayList<String> headCorrect = examine.getHeadList();
					if(headExamine.size()!=headCorrect.size()) {
						error = addError(error, "Different count of result variables.\n");
					}
					else {
						for(int i=0;i<headExamine.size();i++) {
							if(!headExamine.get(i).equals(headCorrect.get(i))) {
								error = addError(error, "Head differs");
							}
						}
					}
				}

				//Check for Order By clause
				if(examine.isSorted()!=correct.isSorted()) {
					error = addError(error, "Trying to compare sorted results to unsorted ones.\n");
				}
				else {
					//Check results
					if(examine.getNrResults()!=correct.getNrResults()) {
						String text = "Number of results expected: " + correct.getNrResults() + "\n";
						text += "Number of results returned: " + examine.getNrResults() + "\n";
						error = addError(error, text);
					}
					else {
						//Only check content for SPARQL results 
						if(!resultsCountOnly) {
							String text = examine.compareQueryResults(correct);
	
							if(text!=null)
								error = addError(error, text);
						}
					}
				}

				if(error==null) {
					correctQueryCount[examine.getQueryNr()-1]++;
				}
				else {
					resultWriter.append("\nResult for Query " + examine.getQueryNr() + " of run " + examine.getRun() + " differs:\n");
					resultWriter.append(error);
				}
				
			  } catch(EOFException e) {
				  resultWriter.append("\n______________________________________________\n\nQualification overview:\n\n");
				    for(int i=0;i<totalQueryCount.length;i++) {
				    	resultWriter.append("Query " + (i+1) + ":");
				    	if(totalQueryCount[i]>0) {
				    		resultWriter.append(" correct/total executions: " + correctQueryCount[i]+"/"+totalQueryCount[i] + "\n");
				    		resultWriter.append(" correct/total ratio:" + 100*correctQueryCount[i]/totalQueryCount[i] + "%\n\n");
				    	}
				    	else
				    		resultWriter.append("Query was not executed or ignored.\n\n");
				    }
				    resultWriter.flush();
				    resultWriter.close();
				    System.out.println("Qualification finished. Results written to " + qualificationLog + ".");
				    return;
			  }
			}
		} catch(IOException e) { e.printStackTrace(); System.exit(-1);}
		  catch(ClassNotFoundException e) { e.printStackTrace(); System.exit(-1); } 
	}
	
	private String addError(String errorString, String error) {
		if(errorString==null)
			errorString = error;
		else
			errorString += error;
		
		return errorString;
	}
	
	private static void printUsageInfo() {
		String output = "Usage: java benchmark.qualification.Qualification <options> Correct.qual Test.qual\n\n" +
		"Correct.qual: file of a correct run\n\n" +
		"Test.qual: file of a run to test against Correct.qual\n\n" +
		"Possible options are:\n" +
		"\t-rc\n" +
		"\t\tOnly check the number of results, not the result content.\n" +
		"\t\tdefault: " + QualificationDefaultValues.resultsCountOnly + "\n" +
		"\t-ql <qualification log file>\n" +
		"\t\tWhere to write the qualification log data into.\n" +
		"\t\tdefault: " + QualificationDefaultValues.qualificationLog + "\n";

		System.out.print(output);
	}
}
