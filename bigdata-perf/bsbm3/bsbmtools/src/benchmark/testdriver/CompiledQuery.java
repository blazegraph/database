package benchmark.testdriver;

public class CompiledQuery {
	private String queryString;
	private byte queryType;
	private int nr;
	private int queryMix;
	
	CompiledQuery(String queryString, byte queryType, int queryNr) {
		this.queryString = queryString;
		this.queryType = queryType;
		this.nr = queryNr;
	}

	public String getQueryString() {
		return queryString;
	}

	public byte getQueryType() {
		return queryType;
	}

	public int getQueryMix() {
		return queryMix;
	}

	public void setQueryMix(int queryMix) {
		this.queryMix = queryMix;
	}

	public int getNr() {
		return nr;
	}
}
