package benchmark.testdriver;

import java.util.Locale;

public class ClientThread extends Thread {
	private PreCalcParameterPool pool;
	private ServerConnection conn;
	private CompiledQueryMix queryMix;
	private ClientManager manager;
	private boolean finishedWarmup;
	private int maxQuery;
	private int nr;
	
	ClientThread(PreCalcParameterPool pool, ServerConnection conn, int maxQuery, ClientManager parent, int clientNr) {
		this.pool = pool;
		this.conn = conn;
		this.maxQuery = maxQuery;
		manager = parent;
		finishedWarmup = false;
		this.nr = clientNr;
	}
	
	@Override
    public void run() {
		queryMix = new CompiledQueryMix(maxQuery);
		while(!Thread.interrupted()) {
			boolean inWarmup;
			boolean inRun;
			boolean gotNew;
			synchronized(manager) {
				gotNew = pool.getNextQueryMix(queryMix);
				inWarmup = manager.isWarmupPhase();
				inRun = manager.isRunPhase();
			}
			
			if(interrupted()) {
				System.err.println("Thread interrupted. Quitting...");
				return;
			}
			//Either the warmup querymixes or the run querymixes ended
			if(!gotNew) {
				try{
					if(inWarmup) {
						//finish work for warmup
						if(!finishedWarmup) {
							manager.finishWarmup(this);
							finishedWarmup = true;
						}
						sleep(20);//still warmup, but no querymix, so wait and try again
						
						continue;
					}
					else if(!inRun) {
						sleep(20);//Run phase didn't start yet, so sleep
						continue;
					}
					else {//The run ended, report results, if there are any
						manager.finishRun(this);
						return;//And end Thread
					}
				} catch(InterruptedException e) {
					System.err.println("Thread interrupted. Quitting...");
					conn.close();
					return;
				}
			}
			else {//else run the Querymix
				Long startTime = System.nanoTime();
				while(queryMix.hasNext()) {
					CompiledQuery next = queryMix.getNext();
					if(next==null || manager.ignoreQueries[next.getNr()-1])
						queryMix.setCurrent(0, -1.0);
					else {
						conn.executeQuery(next,queryMix);
					}
				}
				System.out.println("Thread " + nr + ": query mix " + queryMix.getRun() + ": " + String.format(Locale.US, "%.2f", queryMix.getQueryMixRuntime()*1000)
						+ "ms, total: " + String.format(Locale.US, "%.2f",(System.nanoTime()-startTime)/(double)1000000) + "ms");
				
				queryMix.finishRun();
			}
		}
	}

	public CompiledQueryMix getQueryMix() {
		return queryMix;
	}
}
