package benchmark.testdriver;

public class QueryMix {
	private Query[] queries;
	protected Integer[] queryMix;
	
	private double[] aqet;//arithmetic mean query execution time
	private double[] qmin;//Query minimum execution time
	private double[] qmax;//Query maximum execution time
	private double[] avgResults;
	private double[] aqetg;//Query geometric mean execution time
	private int[] minResults;
	private int[] maxResults;
	private int[] runsPerQuery;//Runs Per Query
	private int[] timeoutsPerQuery;
	private int run;//run: negative values are warm up runs
	
	private int currentQueryIndex;//Index of current query for queryMix
	private int queryMixRuns;//number of query mix runs
	private double queryMixRuntime;//whole runtime of actual run in seconds
	private double minQueryMixRuntime;
	private double maxQueryMixRuntime;
	private double queryMixGeoMean;
	private double totalRuntime;//Total runtime of all runs
	private double multiThreadRuntime;//ClientManager sets this value after the runs
	
	public double getMultiThreadRuntime() {
		return multiThreadRuntime;
	}

	public void setMultiThreadRuntime(double multiThreadRuntime) {
		this.multiThreadRuntime = multiThreadRuntime;
	}

	public QueryMix(Query[] queries, Integer[] queryMix) {
		this.queries = queries;
		this.queryMix = queryMix;
		
		//Queries are enumerated starting with 1
		for(int i=0;i<queries.length;i++) {
			if(queries[i]!=null) {
				queries[i].setNr(i+1);
				queries[i].setQueryMix(this);
			}
		}
		
		//same reason
		for(int i=0;i<queryMix.length;i++) {
			queryMix[i]--;
		}
		
		init();
	}
	
	public void init() {
		aqet = new double[queries.length];
		qmin = new double[queries.length];
		qmax = new double[queries.length];
		
		avgResults = new double[queries.length];
		aqetg = new double[queries.length];
		minResults = new int[queries.length];
		maxResults = new int[queries.length];
		
		runsPerQuery = new int[queries.length];
		timeoutsPerQuery = new int[queries.length];
		
		currentQueryIndex = 0;
		queryMixRuns = 0;
		queryMixRuntime = 0;
		totalRuntime = 0;
		minQueryMixRuntime = Double.MAX_VALUE;
		maxQueryMixRuntime = Double.MIN_VALUE;
		queryMixGeoMean = 0;
		run = 0;

		//Init qmax array
		for(int i=0; i<qmax.length;i++) {
			qmax[i] = Double.MIN_VALUE;
			maxResults[i] = Integer.MIN_VALUE;
		}
		
		//Init qmin array
		for(int i=0; i<qmin.length;i++) {
			qmin[i] = Double.MAX_VALUE;
			minResults[i] = Integer.MAX_VALUE;
		}
	}
	
	/*
	 * Add results of a CompiledQueryMix
	 */
	public synchronized void addCompiledQueryMix(CompiledQueryMix cqMix) {
		int[] cRunsPerQuery = cqMix.getRunsPerQuery();
		double[] cAqet = cqMix.getAqet();
		double[] cAvgResults = cqMix.getAvgResults();
		double[] cAqetg = cqMix.getAqetg();
		double[] cQmin = cqMix.getQmin();
		double[] cQmax = cqMix.getQmax();
		int[] cMinResults = cqMix.getMinResults();
		int[] cMaxResults = cqMix.getMaxResults();
		int[] cTimeouts = cqMix.getTimeoutsPerQuery();
		double cTotalRuntime = cqMix.getTotalRuntime();
		double cMinQueryMixRuntime = cqMix.getMinQueryMixRuntime();
		double cMaxQueryMixRuntime = cqMix.getMaxQueryMixRuntime();
		double cQueryMixGeoMean = cqMix.getQueryMixGeoMean();
		int cQueryMixRuns = cqMix.getQueryMixRuns();
		for(int i=0;i<queries.length;i++) {
			if(cRunsPerQuery[i]>0) {
				int cNrRuns = cRunsPerQuery[i];
				
				
				aqet[i] = (aqet[i]*runsPerQuery[i] + cAqet[i]*cNrRuns)/(runsPerQuery[i]+cNrRuns);
				aqetg[i] += cAqetg[i];
				avgResults[i] = (avgResults[i]*runsPerQuery[i] + cAvgResults[i]*cNrRuns)/(runsPerQuery[i]+cNrRuns);
				timeoutsPerQuery[i] += cTimeouts[i];
				
				if(cQmin[i] < qmin[i])
					qmin[i] = cQmin[i];
				
				if(cQmax[i] > qmax[i])
					qmax[i] = cQmax[i];
				
				if(cMinResults[i] < minResults[i])
					minResults[i] = cMinResults[i];
				
				if(cMaxResults[i] > maxResults[i])
					maxResults[i] = cMaxResults[i];

				runsPerQuery[i] += cNrRuns;
			}
		}
		//QueryMix statistics
		totalRuntime+=cTotalRuntime;
		queryMixGeoMean += cQueryMixGeoMean;
		queryMixRuns+=cQueryMixRuns;
		
		if(cMinQueryMixRuntime < minQueryMixRuntime)
			minQueryMixRuntime = cMinQueryMixRuntime;
		
		if(cMaxQueryMixRuntime > maxQueryMixRuntime)
			maxQueryMixRuntime = cMaxQueryMixRuntime;
	}
	
	public void setRun(int run) {
		this.run = run;
	}
	
	/*
	 * Calculate metrics for this run
	 */
	public void finishRun() {
		currentQueryIndex = 0;
		
		if(run>=0) {
			
			queryMixRuns++;
			
			if(queryMixRuntime < minQueryMixRuntime)
				minQueryMixRuntime = queryMixRuntime;
			
			if(queryMixRuntime > maxQueryMixRuntime)
				maxQueryMixRuntime = queryMixRuntime;
			
			queryMixGeoMean += Math.log10(queryMixRuntime);
			totalRuntime += queryMixRuntime;
		}
		
		//Reset queryMixRuntime
		queryMixRuntime = 0;
	}
	
	public void reportTimeOut() {
		if(run>=0) {
			int queryNr = queryMix[currentQueryIndex];
			timeoutsPerQuery[queryNr]++;
		}
	}
	
	public Query getNext() {
		return queries[queryMix[currentQueryIndex]];
	}
	
	public Boolean hasNext() {
		return currentQueryIndex < queryMix.length;
	}
	
	/*
	 * Set the time (seconds) of the current Query
	 */
	public void setCurrent(int numberResults, Double timeInSeconds) {
		if(run>=0 && timeInSeconds>=0.0) {
			int queryNr = queryMix[currentQueryIndex];
	
			int nrRuns = runsPerQuery[queryNr]++;
			aqet[queryNr] = (aqet[queryNr] * nrRuns + timeInSeconds) / (nrRuns+1);
			avgResults[queryNr] = (avgResults[queryNr] * nrRuns + numberResults) / (nrRuns+1);
			aqetg[queryNr] += Math.log10(timeInSeconds);
			
			if(timeInSeconds < qmin[queryNr])
				qmin[queryNr] = timeInSeconds;
			
			if(timeInSeconds > qmax[queryNr])
				qmax[queryNr] = timeInSeconds;
			
			if(numberResults < minResults[queryNr])
				minResults[queryNr] = numberResults;
			
			if(numberResults > maxResults[queryNr])
				maxResults[queryNr] = numberResults;
				
			queryMixRuntime += timeInSeconds;
		}
		
		currentQueryIndex++;
	}
	

	public Query[] getQueries() {
		return queries;
	}

	public Integer[] getQueryMix() {
		return queryMix;
	}

	public double[] getAqet() {
		return aqet;
	}

	public double[] getQmin() {
		return qmin;
	}

	public double[] getQmax() {
		return qmax;
	}

	public int[] getRunsPerQuery() {
		return runsPerQuery;
	}

	public int getQueryMixRuns() {
		return queryMixRuns;
	}

	public double getQmph() {
		return 3600 / (totalRuntime / queryMixRuns);
	}
	
	public double getMultiThreadQmpH() {
		return 3600 / (multiThreadRuntime / queryMixRuns);
	}

	public double getQueryMixRuntime() {
		return queryMixRuntime;
	}

	public double getTotalRuntime() {
		return totalRuntime;
	}
	
	public double getCQET() {
		return totalRuntime / queryMixRuns;
	}

	public double getMinQueryMixRuntime() {
		return minQueryMixRuntime;
	}

	public double getMaxQueryMixRuntime() {
		return maxQueryMixRuntime;
	}

	public void setQueryMixRuntime(double queryMixRuntime) {
		this.queryMixRuntime = queryMixRuntime;
	}

	public int getRun() {
		return run;
	}

	public double[] getAvgResults() {
		return avgResults;
	}

	public int[] getMinResults() {
		return minResults;
	}

	public int[] getMaxResults() {
		return maxResults;
	}

	public double[] getGeoMean() {
		double[] temp = new double[aqetg.length];
		for(int i=0;i<temp.length;i++)
			temp[i] = Math.pow(10, aqetg[i]/runsPerQuery[i]);

		return temp;
	}

	public double getQueryMixGeometricMean() {
		return Math.pow(10, (queryMixGeoMean/queryMixRuns));
	}

	public int[] getTimeoutsPerQuery() {
		return timeoutsPerQuery;
	}
}
