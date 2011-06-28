package benchmark.qualification;

import java.io.Serializable;
import java.util.*;

public class QueryResult implements Serializable{
	static final long serialVersionUID = 1;
	private int run;
	private int queryNr;
	private String queryText;
	private int nrResults;
	private boolean sorted;
	private HashMap<String,Integer> resultHash;
	private ArrayList<String> resultList;
	private ArrayList<String> headList;
	
	public int getRun() {
		return run;
	}

	public void setRun(int run) {
		this.run = run;
	}

	public QueryResult(int queryNum, String queryText, int nrResults, boolean sorted, ArrayList<String> heads) {
		this.queryNr=queryNum;
		this.queryText=queryText;
		this.nrResults=nrResults;
		this.sorted=sorted;
		this.headList=heads;
		
		if(sorted)
			resultList = new ArrayList<String>(); 
		else
			resultHash = new HashMap<String,Integer>();
	}

	public int getQueryNr() {
		return queryNr;
	}

	public String getQueryText() {
		return queryText;
	}

	public int getNrResults() {
		return nrResults;
	}
	
	public void addResult(String result) {
		if(sorted) 
			resultList.add(result);
		else {
			Integer count = 1;
			if(resultHash.containsKey(result)) {
				count = resultHash.get(result);
				count++;
			}
			resultHash.put(result, count);
		}
	}
	
	/*
	 * Returns null if both QueryResults are the same, otherwise an error message is returned
	 */
	public String compareQueryResults(QueryResult other) {
		String error = null;
		
		if(sorted) {
			error = checkArrayList(other.resultList);
			
		} else {
			error = checkHashMap(other);
		}
		
		return error;
	}
	
	private String checkArrayList(ArrayList<String> otherList) {
		String error=null;
		
		for(int i=0;i<otherList.size();i++) {
			if(!otherList.get(i).equals(resultList.get(i))) {
				error = addError(error, "Wrong results and/or wrong ordering in row " + (i+1) + ".\n");
				error = addError(error, "\tCorrect: " + otherList.get(i));
				error = addError(error, "\n\tFound: " + resultList.get(i) + "\n");
				break;
			}
		}
		return error;
	}
	
	private String checkHashMap(QueryResult other) {
		HashMap<String,Integer> otherMap = other.resultHash;
		String error=null;
		int missing=0;
		int tooMany=0;
		Set<Map.Entry<String, Integer>> keys = otherMap.entrySet();
		Iterator<Map.Entry<String, Integer>> it = keys.iterator();
		
		while(it.hasNext()) {
			String key = it.next().getKey();
			Integer countO = otherMap.get(key);
			
			if(resultHash.containsKey(key)) {
				int count = resultHash.get(key);
				if(count<countO)
					missing += countO - count;
				else if(count>countO)
					tooMany += count - countO;
				resultHash.remove(key);
			}
			else {
				missing += countO;
			}

			tooMany += nrResults - tooMany - other.nrResults + missing ;
		}
		
		if(missing>0)
			error = addError(error, missing + " results are missing. ");
		
		if(tooMany>0)
			error = addError(error, tooMany + " results are incorrect.\n");
		
		return error;
	}
	
	private String addError(String errorString, String error) {
		if(errorString==null)
			errorString = error;
		else
			errorString += error;
		
		return errorString;
	}

	public ArrayList<String> getHeadList() {
		return headList;
	}

	public boolean isSorted() {
		return sorted;
	}
}
