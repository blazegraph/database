package benchmark.generator;

import umontreal.iro.lecuyer.probdist.*;
import java.util.Random;

public class NormalDistGenerator {
	private NormalDistQuick normal;
	private int avg;//The value connected with mu, namely the average value of this generator
	private double mu;
	private Random ranGen;
	
	public NormalDistGenerator(double mu, double sigma, int avgValue, long seed)
	{
		normal 	= new NormalDistQuick(mu,sigma);
		this.mu = mu;
		avg		= avgValue;
		ranGen = new Random(seed);
	}
	
	//Returns 1-x
	public int getValue()
	{
		double randVal = normal.inverseF(ranGen.nextDouble());
		
		while(randVal < 0)
			randVal = normal.inverseF(ranGen.nextDouble());
		
		return (int) ((randVal / mu) * avg) + 1;
	}
	
	//Returns 0-x
	public int getValue0() {
		double randVal = normal.inverseF(ranGen.nextDouble());
		
		while(randVal < 0)
			randVal = normal.inverseF(ranGen.nextDouble());
		
		return (int) ((randVal / mu) * avg);
	}
	
}
