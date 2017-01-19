package saalfeldlab.spark.benchmarks;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import saalfeldlab.spark.benchmarks.action.BenchmarkAction;
import saalfeldlab.spark.benchmarks.action.NoAction;
import scala.Tuple2;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public class Evaluate< T, RDD extends JavaRDDLike< T, RDD > >
{

	private final int N;

	private final JavaSparkContext sc;

	private final RDD rdd;

	public Evaluate( final int n, final JavaSparkContext sc, final RDD rdd )
	{
		super();
		N = n;
		this.sc = sc;
		this.rdd = rdd;
	}

	// rdd should be cached. do it within the call or expect user to do it?
	public void run( final BenchmarkAction< T, RDD > action )
	{

		final long[] times = new long[ N ];
		final long[] noActionTimes = new long[ N ];

		// do noaction test first

		final NoAction< T, RDD > noAction = new NoAction<>();

		for ( int i = 0; i < N; ++i )
		{

			// out should be cached. do it within the call or expect user to do
			// it?
			final Tuple2< JavaRDDLike< ?, ? >, Long > outNoaction = runAction( noAction, rdd );
			final long dtNoAction = outNoaction._2();
			noActionTimes[ i ] = dtNoAction;

			final Tuple2< JavaRDDLike< ?, ? >, Long > out = runAction( action, rdd );
			final long dt = out._2();
			times[ i ] = dt;

			System.out.println( "Action:       dt=" + dt + "ns (" + dt * 1e-9 + "s)" );
			System.out.println( "No action:    dt=" + dtNoAction + "ns (" + dtNoAction * 1e-9 + "s)" );
		}
	}

	private static < T, RDD extends JavaRDDLike< T, RDD > > Tuple2< JavaRDDLike< ?, ? >, Long > runAction( final BenchmarkAction< T, RDD > action, final RDD rdd )
	{
		final JavaRDDLike< ?, ? > out = action.run( rdd );
		final long t0 = System.nanoTime();
		out.count();
		final long t1 = System.nanoTime();
		final long dt = t1 - t0;

		return new Tuple2<>( out, dt );

	}

}
