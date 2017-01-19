package saalfeldlab.spark.benchmarks.action;

/**
 * @author Philipp Hanslovsky
 */

import org.apache.spark.api.java.JavaRDDLike;

public class NoAction< T, RDD extends JavaRDDLike<T, RDD> > implements
BenchmarkAction< T, RDD > {


@Override
public RDD run(RDD rdd) {
	return rdd;
}
}
