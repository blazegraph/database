package benchmark.generator;

import umontreal.iro.lecuyer.probdist.*;
import java.util.Random;

/*
 * A Number Generator over the normal distributed range 1-max
 */
public class NormalDistRangeGenerator {
	private NormalDistQuick normal;
	private int max;
	private double normalLimit;
	private Random ranGen;
	
	public NormalDistRangeGenerator(double mu, double sigma, int maxValue, double normalLimit, long seed)
	{
		normal 	= new NormalDistQuick(mu,sigma);
		max		= maxValue;
		this.normalLimit = normalLimit;
		ranGen = new Random(seed);
	}
	
	public int getValue()
	{
		double randVal = normal.inverseF(ranGen.nextDouble());
		
		while(randVal > normalLimit || randVal < 0)
			randVal = normal.inverseF(ranGen.nextDouble());
		
		return (int) ((randVal / normalLimit) * max + 1);
	}
}
