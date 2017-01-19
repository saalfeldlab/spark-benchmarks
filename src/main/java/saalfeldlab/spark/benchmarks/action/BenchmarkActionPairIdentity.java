package saalfeldlab.spark.benchmarks.action;

import org.apache.spark.api.java.JavaPairRDD;

public class BenchmarkActionPairIdentity< K, V > implements BenchmarkActionPair< K, V >
{

	@Override
	public JavaPairRDD< K, V > run( final JavaPairRDD< K, V > rdd )
	{
		return rdd.mapToPair( t -> t );
	}

}
