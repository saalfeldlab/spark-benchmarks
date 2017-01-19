package saalfeldlab.spark.benchmarks.action;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

public class BenchmarkActionIdentity< V, RDD extends JavaRDDLike< V, RDD > > implements BenchmarkAction< V, RDD >
{

	@Override
	public JavaRDD< V > run( final RDD rdd )
	{
		return rdd.map( val -> val );
	}

}
