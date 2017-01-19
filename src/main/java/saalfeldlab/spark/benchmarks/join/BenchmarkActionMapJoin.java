package saalfeldlab.spark.benchmarks.join;

import org.apache.spark.api.java.JavaPairRDD;

import saalfeldlab.spark.benchmarks.action.BenchmarkActionPair;
import scala.Tuple2;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class BenchmarkActionMapJoin< V > implements BenchmarkActionPair< Integer, V >
{

	private final int[] map;

	public BenchmarkActionMapJoin( final int[] map )
	{
		super();
		this.map = map;
	}

	@Override
	public JavaPairRDD< Integer, V > run( final JavaPairRDD< Integer, V > rdd )
	{
		final int[] map = this.map;
		return rdd.mapToPair( t -> new Tuple2<>( map[ t._1() ], t._2() ) );
	}

}
