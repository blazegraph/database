package benchmark.testdriver;

public class CompiledQueryMix {
	protected CompiledQuery[] queryMix;
	int queryNr;
	
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
	
	/*
	 * Initialize the CompileQueryMix with CompiledQueries and a max query number for array sizes
	 */
	public CompiledQueryMix(CompiledQuery[] queryMix, int maxQueryNr) {
		this.queryNr = maxQueryNr;
		this.queryMix = queryMix;
		init();
	}
	
	public CompiledQueryMix(int maxQueryNr) {
		this.queryNr = maxQueryNr;
		
		init();
	}
	
	public void setNewCompiledQueryMix(CompiledQuery[] queryMix) {
		this.queryMix = queryMix;
	}
	
	/*
	 * After every run this method initializes the CompiledQueryMix with a new CompiledQuery set.
	 * All metrics are set to the start values.
	 */
	public void init(CompiledQuery[] queryMix) {
		this.queryMix = queryMix;
		
		init();
	}
	
	public void init() {
		aqet = new double[queryNr];
		qmin = new double[queryNr];
		qmax = new double[queryNr];
		
		avgResults = new double[queryNr];
		aqetg = new double[queryNr];
		minResults = new int[queryNr];
		maxResults = new int[queryNr];
		
		runsPerQuery = new int[queryNr];
		timeoutsPerQuery = new int[queryNr];
		
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
		queryMix = null;
	}
	
	public CompiledQuery getNext() {
		return queryMix[currentQueryIndex];
	}
	
	public Boolean hasNext() {
		return currentQueryIndex < queryMix.length;
	}
	
	public void reportTimeOut() {
		int queryNr = queryMix[currentQueryIndex].getNr()-1;
		timeoutsPerQuery[queryNr]++;
	}
	
	/*
	 * Set the time (seconds) of the current Query
	 */
	public void setCurrent(int numberResults, Double timeInSeconds) {
		if(run>=0 && timeInSeconds>=0.0) {
			int queryNr = queryMix[currentQueryIndex].getNr()-1;
	
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

	public double getQueryMixGeoMean() {
		return queryMixGeoMean;
	}

	public double[] getAqetg() {
		return aqetg;
	}

	public int[] getTimeoutsPerQuery() {
		return timeoutsPerQuery;
	}
}
