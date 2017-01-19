package saalfeldlab.spark.benchmarks.action;

import org.apache.spark.api.java.JavaRDDLike;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class NoAction< T, RDD extends JavaRDDLike< T, RDD > > implements BenchmarkAction< T, RDD >
{

	@Override
	public RDD run( final RDD rdd )
	{
		return rdd;
	}
}
