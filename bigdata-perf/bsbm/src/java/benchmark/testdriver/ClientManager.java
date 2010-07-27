package benchmark.testdriver;

public class ClientManager {
	private int activeThreadsInWarmup;
	private int activeThreadsInRun;
	private boolean warmupPhase;
	private boolean runPhase;
	private int nrThreads;
	private int nrWarmup;
	private QueryMix queryMix;
	private PreCalcParameterPool pool;
	protected boolean[] ignoreQueries;
	private ClientThread[] clients;
	private TestDriver parent;
	
	ClientManager(AbstractParameterPool pool, TestDriver parent) {
		activeThreadsInWarmup = 0;
		activeThreadsInRun = 0;
		this.parent = parent;
		this.nrWarmup = parent.warmups;
		this.nrThreads = parent.nrThreads;
		this.queryMix = parent.queryMix;
		this.ignoreQueries = parent.ignoreQueries;
		
		this.pool = new PreCalcParameterPool(parent.parameterPool, nrWarmup);
		this.pool.calcQueryMixes(queryMix, parent.nrRuns+nrWarmup);
		queryMix.init();//Reset the Query Mix for further use
	}
	
	public void createClients() {
		clients = new ClientThread[nrThreads];
		for(int i=0;i<nrThreads;i++) {
			ServerConnection sConn;
			if(parent.doSQL)
				sConn = new SQLConnection(parent.sparqlEndpoint, parent.timeout, parent.driverClassName);
			else
				sConn = new SPARQLConnection(parent.sparqlEndpoint, parent.defaultGraph, parent.timeout);
				
			clients[i] = new ClientThread(pool, sConn, ignoreQueries.length, this, i+1);
		}
		System.out.println("Clients created.");
		System.out.flush();
	}
	
	/*
	 * warmup run
	 */
	public void startWarmup() {
		warmupPhase = true;
		activeThreadsInWarmup = nrThreads;
		for(int i=0; i<nrThreads;i++)
			clients[i].start();
		while(activeThreadsInWarmup>0) {
			try {
				Thread.sleep(10);
			}
			catch(InterruptedException e) {
				System.err.println("Got interrupted. Exit.");
				return;
			}
		}
		
		System.out.println("Warmup phase ended...\n");
		warmupPhase = false;
		return;
	}
	
	/*
	 * start actual run
	 */
	public void startRun() {
		System.out.println("Starting actual run...");
		Long start;
		activeThreadsInRun = nrThreads;

		synchronized(pool) {
			runPhase = true;
			pool.setRunPhase();
			start = System.nanoTime();
		}

		while(activeThreadsInRun>0) {
			try {
				Thread.sleep(50);
			}
			catch(InterruptedException e) {
				System.err.println("Got interrupted. Exit.");
				return;
			}
		}
		Long stop = System.nanoTime();
		Double totalRunTimeInSeconds = (stop - start)/(double)1000000000;
		queryMix.setMultiThreadRuntime(totalRunTimeInSeconds);
		System.out.println("Benchmark run completed in " + totalRunTimeInSeconds + "s");
		return;
	}
	
	/*
	 * If a client has finished its Warmup runs it should call this function
	 */
	public synchronized void finishWarmup(ClientThread client) {
		client.getQueryMix().init();
		activeThreadsInWarmup--;
	}

	/*
	 * If a client is finished it reports its results to the ClientManager
	 */
	public synchronized void finishRun(ClientThread client) {
		CompiledQueryMix qMix = client.getQueryMix();
		if(qMix.getQueryMixRuns()>0)
			this.queryMix.addCompiledQueryMix(qMix);
		activeThreadsInRun--;
	}

	public boolean isWarmupPhase() {
		return warmupPhase;
	}

	public boolean isRunPhase() {
		return runPhase;
	}
}
