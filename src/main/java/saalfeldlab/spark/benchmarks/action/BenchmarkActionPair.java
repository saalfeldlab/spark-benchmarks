package saalfeldlab.spark.benchmarks.action;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public interface BenchmarkActionPair< K, V > extends BenchmarkAction< Tuple2< K, V >, JavaPairRDD< K, V > >
{

}
