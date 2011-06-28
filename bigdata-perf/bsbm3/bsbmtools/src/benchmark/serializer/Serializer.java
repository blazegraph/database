package benchmark.serializer;

public interface Serializer {
	
	public void gatherData(ObjectBundle bundle);
	
	public void serialize();
	
	public Long triplesGenerated();
}
