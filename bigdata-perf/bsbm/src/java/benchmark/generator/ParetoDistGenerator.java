package benchmark.generator;

import umontreal.iro.lecuyer.probdist.ParetoDist;

public class ParetoDistGenerator {
	private ParetoDist pareto;
	private int max;
	
	public ParetoDistGenerator(double alpha, int maxValue)
	{
		pareto 	= new ParetoDist(alpha);
		max		= maxValue;
	}
	
	public int getValue()
	{
		int i;
		for(i=(int)pareto.inverseF(Math.random());i>max;i=(int)pareto.inverseF(Math.random()));
		return i;
	}
}
