/**
 * by Yuanbo Guo
 * Semantic Web and Agent Technology Lab, CSE Department, Lehigh University, USA
 * Copyright (C) 2004
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */

package edu.lehigh.swat.bench.ubt;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.config.NicUtil;

import edu.lehigh.swat.bench.ubt.api.QueryResult;
import edu.lehigh.swat.bench.ubt.api.Repository;

public class Test extends RepositoryCreator {
  /**
   * iff a warm up query should be run. warm up can be useful since it will
   * cause the java classes to be more or less loaded and that helps to
   * eliminate some weirdness.
   */
  public static final boolean warmUp;
  static {
      warmUp = Boolean.parseBoolean(System.getProperty("lubm.warmUp","true"));
      System.out.println("lubm.warmUp"+"="+warmUp);
  }
  /** execution times of each query (should be 10 for benchmark runs)*/
  public static final int QUERY_TIME;
  static {
      QUERY_TIME = Integer.parseInt(System.getProperty("lubm.queryTime","10"));
      System.out.println("lubm.queryTime"+"="+QUERY_TIME);
  }
  /**
     * The #of queries to present in parallel each time a query is presented.
     * This is basically the concurrent query load while {@link #QUERY_TIME} is
     * the #of trials. The total #of timees a given query is presented is
     * therefore {@link #QUERY_TIME} x {@link #QUERY_PARALLEL}.
     */
  public static final int QUERY_PARALLEL;
  static {
      QUERY_PARALLEL = Integer.parseInt(System.getProperty("lubm.queryParallel","1"));
      System.out.println("lubm.queryParallel"+"="+QUERY_PARALLEL);
  }
  /** name of file to hold the query test result */
  private static final String QUERY_TEST_RESULT_FILE;
    static {
    	final String resultFile = System.getProperty("lubm.resultFile");
    	if(resultFile== null) {
        /*
         * Note: the hostname is used as part of the filename to help keep track
         * of the results and to allow runs when the test harness is on an
         * shared volume (but you must keep distinct config.kb files for each
         * run and avoid editing the shell script while it is executing).
         */
        String hostname;
        try {
            hostname = NicUtil.getIpAddress("default.nic", "default", false);
        } catch(Throwable t) {//for now, maintain same failure logic as used previously
            t.printStackTrace();
            hostname = NicUtil.getIpAddressByLocalHost();
        }
        QUERY_TEST_RESULT_FILE = hostname + "-result.txt";
    	} else {
    	QUERY_TEST_RESULT_FILE = resultFile;
    	}
  }

  /** list of target systems */
  private Vector kbList_; //KbSpec
  /** list of test queries */
  private Vector queryList_; //QuerySpec
  /** result holder of query test */
  private QueryTestResult[][] queryTestResults_;
  /** Output stream for query test results. The file is created simply to ease the
   * manipulation of the results lateron, e.g., copying it to a Excel sheet.
   */
  private PrintStream queryTestResultFile_;
  /** flag indicating whether the target system is memory-based or not */
  private boolean isMemory_ = false;
  /** the current repository object */
  private Repository repository_ = null;

  /**
   * main method
   */
  public static void main (String[] args) {
    String arg = "";

    try {
      if (args.length < 1) throw new Exception();
      arg = args[0];
      if (arg.equals("query")) {
        if (args.length < 3) throw new Exception();
      }
      else if (arg.equals("load")) {
        if (args.length < 2) throw new Exception();
      }
      else if (arg.equals("memory")) {
        if (args.length < 3) throw new Exception();
      }
      else throw new Exception();
    }
    catch (Exception e) {
      System.err.println("Usage: Test load <kb config file>");
      System.err.println("    or Test query <kb config file> <query config file>");
      System.err.println("    or Test memory <kb config file> <query config file>");
      System.exit(-1);
    }

      Test test = new Test();
      if (arg.equals("query")) {
        test.testQuery(args[1], args[2]);
      }
      else if (arg.equals("load")) {
        test.testLoad(args[1]);
      }
      else if (arg.equals("memory")) {
        test.testMemory(args[1], args[2]);
      }

    System.exit(0);
  }

  /**
   * constructor.
   */
  public Test() {
  }

  /**
   * Starts the loading test defined in the specified config file.
   * @param kbConfigFile The knowledge base config file describing the target systems
   * and test data.
   */
  public void testLoad(String kbConfigFile) {
    createKbList(kbConfigFile);
    doTestLoading();
  }

  /**
   * Starts the query test defined in the specified config files.
   * @param kbConfigFile The knowledge base config file describing the target systems.
   * @param queryConfigFile The query config file describing the test queries.
   */
  public void testQuery(String kbConfigFile, String queryConfigFile) {
    try {
      queryTestResultFile_ = new PrintStream(new FileOutputStream(QUERY_TEST_RESULT_FILE));
      createKbList(kbConfigFile);
      createQueryList(queryConfigFile);
      doTestQuery();
      queryTestResultFile_.flush();
      queryTestResultFile_.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Starts the test on a memory-based system. Query test is conducted immediately
   * after loading.
   * NOTE: The current implementation is only workable in the case where there is
   * just one single system in the test, i.e., only one system is defined in the
   * config file.
   * @param kbConfigFile The knowledge base config file describing the target systems.
   * @param queryFile The query config file describing the test queries.
   */
  public void testMemory(String kbConfigFile, String queryFile) {
    isMemory_ = true;
    testLoad(kbConfigFile);
    testQuery(kbConfigFile, queryFile);
  }

  /**
   * Creates the target system list from the config file.
   * @param kbConfigFile The knowledge base config file.
   */
  private void createKbList(String kbConfigFile) {
    try {
      kbList_ = new KbConfigParser().createKbList(kbConfigFile, isMemory_);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /** Creates the query list from the config file
   * @param queryConfigFile The query config file.
   */
  private void createQueryList(String queryConfigFile) {
    try {
      queryList_ = new QueryConfigParser().createQueryList(queryConfigFile);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Conducts the loading test.
   */
  private void doTestLoading() {
    KbSpecification kb;
    Date startTime, endTime;

    for (int i = 0; i < kbList_.size(); i++) {
      kb = (KbSpecification)kbList_.get(i);

      repository_ = createRepository(kb.kbClass_,kb.dbFile_);
      if (repository_ == null) {
        System.err.println(kb.kbClass_ + ": class not found!");
        System.exit(-1);
      }

      repository_.setOntology(kb.ontology_);

      System.out.println();
      System.out.println("Started loading " + kb.id_);

      startTime = new Date();
      if (isMemory_) repository_.open(null);
      else repository_.open(kb.dbFile_);
      if (!repository_.load(kb.dataDir_)) {
        repository_.close();
        return;
      }
      if (!isMemory_) repository_.close();

      endTime = new Date();
      kb.duration = (endTime.getTime() - startTime.getTime())/1000;
      kbList_.set(i, kb);
      System.out.println();
      System.out.println();
      System.out.println("Finished loading " + kb.id_ + "\t" + kb.duration + " seconds");
    }

    showTestLoadingResults();
  }

  /** Conducts query test */
  private void doTestQuery() {
    queryTestResults_ = new QueryTestResult[kbList_.size()][queryList_.size()];
    for (int i = 0; i < kbList_.size(); i++) {
      testQueryOneKb(i);
    }
    showTestQueryResults();
  }

  /**
   * Conducts query test on the specified system.
   * @param index Index of the system in the target system list.
   */
  private void testQueryOneKb(int index) {
    QuerySpecification query;
    Date startTime, endTime;
    long duration = 0l, sum;
    KbSpecification kb;
    long resultNum = 0;
    
    kb = (KbSpecification)kbList_.get(index);

    if (!isMemory_) repository_ = createRepository(kb.kbClass_,kb.dbFile_);
    if (repository_ == null) {
      System.err.println(kb.kbClass_ + ": class not found!");
      System.exit(-1);
    }

    //set ontology
    repository_.setOntology(kb.ontology_);

    // warm up - required to get Java classes loaded.
    if(!isMemory_&&warmUp){

        QueryResult result;

        repository_.open(kb.dbFile_);
        query = (QuerySpecification)queryList_.get(0);
        System.out.println();
        System.out.println("WARMING UP WITH: ~~~" + query.id_ + "~~~");
//        //issue the query for QUERY_TIME times
//        int j;
//        for (j = 0; j < QUERY_TIME; j++) {
//          startTime = new Date();
          result = repository_.issueQuery(query.query_);
          if (result == null) {
            repository_.close();
            System.err.println("Query error!");
            System.exit(-1);
          }
          //traverse the result set.
          resultNum = 0;
          while (result.next() != false) resultNum++;
//          endTime = new Date();
//          duration = endTime.getTime() - startTime.getTime();
//          sum += duration;
//          System.out.println("\tDuration: " + duration + "\tResult#: " + resultNum);
//        } //end of for j
        //close the repository
        repository_.close();
        
    }
    
    System.out.println();
    System.out.println("### Started testing " + kb.id_ + " ###");
    queryTestResultFile_.println(kb.id_);

    //test each query on this repository
    for (int i = 0; i < queryList_.size(); i++) {
      //open repository
//        System.err.println("open: "+kb.dbFile_);
      if (!isMemory_) repository_.open(kb.dbFile_);
      sum = 0l;
      query = (QuerySpecification)queryList_.get(i);
      System.out.println();
      System.out.println("~~~" + query.id_ + "~~~");
      //issue the query for QUERY_TIME times
      int j;
      int nfailed = 0;
      long firstResultCount = 0;
      boolean consistent = true;
      for (j = 0; j < QUERY_TIME; j++) {
//          System.err.println("issueQuery: "+query.id_);
        startTime = new Date();
        /*
         * New logic runs the N instances of the query on the service
         * and reports the average for those N runs.
         */
        if(QUERY_PARALLEL<=0) throw new IllegalStateException("QUERY_PARALLEL must be GTE ONE.");
        final ExecutorService service = Executors.newFixedThreadPool(
                        QUERY_PARALLEL, new DaemonThreadFactory(getClass()
                                .getName()
                                + ".queryService"));
        try {
        final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(
                    QUERY_PARALLEL);
        final long[] resultCounts = new long[QUERY_PARALLEL];
        for (int x = 0; x < QUERY_PARALLEL; x++) {
            tasks.add(new QueryTask(repository_, query));
        }
        final List<Future<Long/*resultNum*/>> futures;
        try {
            futures = service.invokeAll(tasks);
        } catch (InterruptedException ex) {
            // service was interrupted (unlikely)
            throw new RuntimeException(ex);
        }
        {
        int x = 0;
        for (Future<Long/*resultNum*/> f : futures) {
            try {
                // get future for each task.
                final long tmp = f.get();
                resultCounts[x++] = tmp;
            } catch (Throwable t) {
                /*
                 * Note: report errors rather than crashing the test
                 * harness.
                 */
                t.printStackTrace(System.err);
                nfailed++;
            }
        }
        }
        /*
         * Figure out if resultCounts are consistent and report #of results.
         * 
         * Note: #of results is reported for the first trial. Duration is the
         * total time required to execute the queries concurrently.
         */
        {
        for(int x=0; x<QUERY_PARALLEL; x++) {
            if(x>0) {
                if(resultCounts[x]!=resultCounts[0]) {
                    System.err.println("INCONSISTENT RESULTS IN PARALLEL TRIAL: query="+query.id_+", resultCounts="+Arrays.toString(resultCounts));
                    break;
                }
            }
        }
//        System.out.println("query="+query.id_+", resultCounts="+Arrays.toString(resultCounts));
        resultNum = resultCounts[0];
        }

// Note: old logic runs query in the main thread.
//        try {
//        result = repository_.issueQuery(query.query_);
//        if (result == null) {
//          repository_.close();
//          System.err.println("Query error!");
//          System.exit(-1);
//        }
//        //traverse the result set.
//        resultNum = 0;
//        while (result.next() != false) resultNum++;
//        } catch(Throwable t) {
//            // note: modified to report errors rather than crashing the test harness bbt 9/22/08
//            t.printStackTrace(System.err);
//            nfailed++;
//        }
        endTime = new Date();
        duration = endTime.getTime() - startTime.getTime();
        sum += duration;
        if (j == 0) {
            firstResultCount = resultNum;
        } else {
            if (resultNum != firstResultCount) {
                consistent = false;
            }
        }
        System.out.println("\tQuery: "+query.id_+"\t Duration: " + duration + "\tResult#: " + resultNum
                +(consistent?"":"\t***INCONSISTENT***"));
        } finally {
            // shutdown the thread pool.
            service.shutdownNow();
            try {
                while(!service.awaitTermination(2, TimeUnit.SECONDS)) {
                    System.err.println("Waiting for service shutdown.");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted",e);
            }
        }
      } //end of for j
      //close the repository
      if (!isMemory_) repository_.close();
      //record the result
      queryTestResults_[index][i] = new QueryTestResult();
      if (nfailed == 0) {
          queryTestResults_[index][i].duration_ = sum/j;
          queryTestResults_[index][i].resultNum_ = resultNum;
      } else {
          queryTestResults_[index][i].duration_ = Long.MAX_VALUE;
          queryTestResults_[index][i].resultNum_ = Long.MIN_VALUE;
      }
      queryTestResults_[index][i].consistent = consistent;
//      result = null;
      /*
       * Note: modified as a tab-delimited format bbt 9/15/2008
       */
      queryTestResultFile_.print(query.id_+"\t");
      queryTestResultFile_.print(sum/j+"\t");
      queryTestResultFile_.print(resultNum);
      if(!consistent) {
          queryTestResultFile_.print("\tINCONSISTENT");
      }
      queryTestResultFile_.println();
      //queryTestResultFile_.println();
    } //end of for i
    System.out.println("### Finished testing " + kb.id_ + " ###");
  }

    /**
     * Run a query.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class QueryTask implements Callable<Long> {

        private final QuerySpecification query;

        private final Repository repository;

        /**
         * 
         * @param repository
         *            The repo.
         * @param query
         *            The query.
         */
        public QueryTask(final Repository repository,
                final QuerySpecification query) {

            this.query = query;

            this.repository = repository;

        }

        /**
         * Run a query.
         * 
         * @return The #of results reported by the query.
         */
        public Long call() throws Exception {

            final QueryResult result = repository.issueQuery(query.query_);

            if (result == null) {

                // null is protocol for repository failure on query.
                throw new RuntimeException("Query returned null: query="
                        + query.id_);

            }

            // traverse the result set.
            long resultNum = 0;

            while (result.next() != false)
                resultNum++;

            return resultNum;

        }

    }
    
  /**
     * Displays the loading test results.
     */
  private void showTestLoadingResults() {
    KbSpecification kb;

    System.out.println();
    for (int i = 0; i < kbList_.size(); i++) {
      kb = (KbSpecification)kbList_.get(i);
      System.out.println("\t" + kb.id_ + "\t" + kb.duration + " seconds");
    }
  }

  /**
   * Displays the query test results.
   */
  private void showTestQueryResults() {
    KbSpecification kb;
    QuerySpecification query;
    QueryTestResult result;

    System.out.println();
    for (int i = 0; i < kbList_.size(); i++) {
      kb = (KbSpecification)kbList_.get(i);
      System.out.print(kb.id_+"\t#trials="+QUERY_TIME+"\t#parallel="+QUERY_PARALLEL);
    }
    System.out.println();
    System.out.println("query\tTime\tResult#");
    long totalDuration = 0;
    for (int j = 0; j < queryList_.size(); j++) {
      query = (QuerySpecification)queryList_.get(j);
      System.out.print(query.id_);
      for (int i = 0; i < kbList_.size(); i++) {
        kb = (KbSpecification)kbList_.get(i);
        result = queryTestResults_[i][j];
        totalDuration += result.duration_;
        System.out.print("\t" + result.duration_ + "\t" + result.resultNum_);
        if(!result.consistent) {
            System.out.print("\tINCONSISTENT");
        }
      }
      System.out.println();
    }
    System.out.println("Total\t"+totalDuration);
  }
}