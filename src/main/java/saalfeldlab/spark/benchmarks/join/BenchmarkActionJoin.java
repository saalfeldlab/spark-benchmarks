package saalfeldlab.spark.benchmarks.join;

import org.apache.spark.api.java.JavaPairRDD;

import saalfeldlab.spark.benchmarks.action.BenchmarkActionPair;
import scala.Tuple2;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class BenchmarkActionJoin< K, V1, V2 > implements BenchmarkActionPair< K, V2 >
{

	private final JavaPairRDD< K, V1 > joiner;

	public BenchmarkActionJoin( final JavaPairRDD< K, V1 > joiner )
	{
		super();
		this.joiner = joiner;
	}

	@Override
	public JavaPairRDD< K, Tuple2< V1, V2 > > run( final JavaPairRDD< K, V2 > joinee )
	{
		return joiner.join( joinee );
	}

}
