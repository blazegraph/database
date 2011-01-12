package benchmark.testdriver;

public class PreCalcParameterPool {
	AbstractParameterPool parameterPool;
	private CompiledQuery queryMixes[][];
	private int queryMixNr;
	private int warmups;
	private boolean warmupPhase;
	private boolean runPhase;
	
	PreCalcParameterPool(AbstractParameterPool parameterPool, int warmups) {
		this.parameterPool = parameterPool;
		this.warmups = warmups;
		warmupPhase = true;
		runPhase = false;
		queryMixNr = 0;
	}
	
	/*
	 * Calculate query mixes with all queries for the test run
	 */
	public void calcQueryMixes(QueryMix queryMix, int times) {
		System.out.print("Generating queries...");
		System.out.flush();
		queryMixes = new CompiledQuery[times][queryMix.queryMix.length];
		
		for(int nrRun=0;nrRun<times;nrRun++) {
			queryMix.setRun(nrRun);
			int i=0;
			while(queryMix.hasNext()) {
				Query next = queryMix.getNext();
				//Don't create queries for the warm-up phase
				if(nrRun < warmups && next.getQueryType()==Query.UPDATE_TYPE) {
					queryMixes[nrRun][i++] = null;
					queryMix.setCurrent(0, -1.0);
					continue;
				}
				Object[] queryParameters = parameterPool.getParametersForQuery(next);
				next.setParameters(queryParameters);
				
				queryMixes[nrRun][i++] = new CompiledQuery(next.getQueryString(), next.getQueryType(), next.getNr());
				
				queryMix.setCurrent(0, -1.0);
			}
			queryMix.finishRun();
		}
		System.out.println("done");
	}
	
	public synchronized CompiledQuery[] getNextQueryMix() {
		if(queryMixNr==warmups)
			warmupPhase = false;
		
		if(!warmupPhase && !runPhase) {
			return null;
		}
		
		if(queryMixNr >= queryMixes.length)
			return null;
		else {
			return queryMixes[queryMixNr++];
		}
	}
	public synchronized boolean getNextQueryMix(CompiledQueryMix queryMix) {
		if(queryMixNr==warmups)
			warmupPhase = false;
		
		if(!warmupPhase && !runPhase) {
			return false;
		}
		
		if(queryMixNr >= queryMixes.length)
			return false;
		else {
			queryMix.setRun(queryMixNr-warmups);
			queryMix.setNewCompiledQueryMix(queryMixes[queryMixNr++]);
			return true;
		}
	}

	public boolean isWarmupPhase() {
		return warmupPhase;
	}

	public void setRunPhase() {
		this.runPhase = true;
	}

	public boolean isRunPhase() {
		return runPhase;
	}
}
