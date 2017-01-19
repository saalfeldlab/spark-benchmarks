package saalfeldlab.spark.benchmarks.action;

import org.apache.spark.api.java.JavaRDDLike;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public interface BenchmarkAction< T, IN extends JavaRDDLike< T, IN > >
{

	public JavaRDDLike< ?, ? > run( IN rdd );

}
