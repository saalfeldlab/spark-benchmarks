package saalfeldlab.spark.benchmarks.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import saalfeldlab.spark.benchmarks.Evaluate;
import scala.Tuple2;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class JoinBenchmark
{

	public static < V > void run( final JavaSparkContext sc, final JavaRDD< Integer > in, final int[][] mapping, final DataCreator< Integer, V > creator, final int N )
	{

		final JavaPairRDD< Integer, V > big = in.mapToPair( k -> new Tuple2<>( k, creator.create( k ) ) ).cache();
		big.count();

		final Broadcast< int[][] > mappingBC = sc.broadcast( mapping );

		final JavaPairRDD< Integer, Integer > small = in.mapToPair( k -> new Tuple2<>( k, mappingBC.getValue()[ k ] ) ).flatMapToPair( t -> {
			final Iterator< Integer > it = IntStream.of( t._2() ).boxed().collect( Collectors.toList() ).iterator();
			return new Iterator< Tuple2< Integer, Integer > >()
			{

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< Integer, Integer > next()
				{
					return new Tuple2<>( t._1(), it.next() );
				}
			};
		} ).mapToPair( t -> t.swap() ).cache();

		small.count();

		System.out.println( "big.join(small)" );
		new Evaluate<>( N, sc, small ).run( new BenchmarkActionJoin<>( big ) );
		System.out.println();

		System.out.println( "small.join(big)" );
		new Evaluate<>( N, sc, big ).run( new BenchmarkActionJoin<>( small ) );
		System.out.println();

		final int[] fwdMap = new int[ mapping.length ];
		for ( int i = 0; i < mapping.length; ++i )
		{
			final int[] m = mapping[ i ];
			for ( int k = 0; k < m.length; ++k )
				fwdMap[ m[ k ] ] = i;
		}

		System.out.println( "mapside join" );
		new Evaluate<>( N, sc, big ).run( new BenchmarkActionMapJoin<>( fwdMap ) );

	}

	public static void main( final String[] args )
	{

		final int count = Integer.parseInt( args[ 0 ] );
		final int arraySize = Integer.parseInt( args[ 1 ] );
		final int N = Integer.parseInt( args[ 2 ] );
		final String master = args.length > 3 ? args[ 3 ] : "local[*]";

		final DataCreator< Integer, float[] > creator = ( Serializable & DataCreator< Integer, float[] > ) i -> new float[ arraySize ];
		final int[][] mapping = new int[ count ][];
		final ArrayList< Integer > indices = new ArrayList<>();

		for ( int i = 0, cnt = count; i < mapping.length; ++i, cnt -= 2 )
		{
			indices.add( i );
			mapping[ i ] = cnt > 0 ? new int[] { cnt, cnt - 1 } : new int[ 0 ];
		}

		final SparkConf conf = new SparkConf().setMaster( master ).setAppName( "JoinBenchmark" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		final JavaRDD< Integer > in = sc.parallelize( indices ).cache();
		in.count();

		run( sc, in, mapping, creator, N );

	}

}
