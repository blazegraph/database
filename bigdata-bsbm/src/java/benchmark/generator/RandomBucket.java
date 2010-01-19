package benchmark.generator;

import java.util.Random;

public class RandomBucket {
	
	private double[] cumulativePercentage;
	private Object[] objects;
	private int index;
	private double totalPercentage;
	private Random ranGen;
	
	public RandomBucket(int size)
	{
		cumulativePercentage = new double[size];
		objects = new Object[size];
		index=0;
		totalPercentage = 0.0;
		ranGen = new Random();
	}
	
	public RandomBucket(int size, long seed)
	{
		cumulativePercentage = new double[size];
		objects = new Object[size];
		index=0;
		totalPercentage = 0.0;
		ranGen = new Random(seed);
	}
	
	public void add(double percentage, Object obj)
	{
		if(index==objects.length)
		{
			System.err.println("No more objects can be added into Bucket!");
			return;
		}
		else
		{
			objects[index] = obj;
			cumulativePercentage[index] = percentage;
			totalPercentage+=percentage;
		}
		
		index++;
		
		if(index==objects.length)
		{
			double cumul=0.0;
			for(int i=0;i<objects.length;i++)
			{
				cumul += cumulativePercentage[i]/totalPercentage;
				cumulativePercentage[i] = cumul;
			}
		}
	}
	
	public Object getRandom()
	{
		double randIndex = ranGen.nextDouble();
		
		for(int i=0;i<objects.length;i++)
		{
			if(randIndex<=cumulativePercentage[i])
				return objects[i];
		}
		//Should never happens, but...
		return objects[objects.length-1];
	}
}
