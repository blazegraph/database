package benchmark.testdriver;

public abstract class AbstractParameterPool {
	protected Integer scalefactor; 
	
	public abstract Object[] getParametersForQuery(Query query);
	
	public Integer getScalefactor() {
		return scalefactor;
	}
}
