package saalfeldlab.spark.benchmarks;

import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;

import saalfeldlab.spark.benchmarks.action.BenchmarkAction;
import saalfeldlab.spark.benchmarks.action.NoAction;
import scala.Tuple2;

public class Evaluate {
	
	private final int N;
	
	public Evaluate(int N) {
		super();
		this.N = N;
	}

	// rdd should be cached. do it within the call or expect user to do it?
	public <T, RDD extends JavaRDDLike< T, RDD > > void run(
			JavaSparkContext sc, 
			RDD rdd, 
			BenchmarkAction< T, RDD > action ) {
		
		long[] times = new long[ N ];
		long[] noActionTimes = new long[ N ];
		
		// do noaction test first
		
		NoAction<T, RDD> noAction = new NoAction<>();
		
		for ( int i = 0; i < N; ++i ) {
			
			
			
			// out should be cached. do it within the call or expect user to do it?
			Tuple2<JavaRDDLike<?, ?>, Long> outNoaction = runAction( noAction, rdd );
			long dtNoAction = outNoaction._2();
			noActionTimes[ i ] = dtNoAction;
			
			Tuple2<JavaRDDLike<?, ?>, Long> out = runAction( action, rdd );
			long dt = out._2();
			times[ i ] = dt;
			
			System.out.println( "Action:       dt=" + dt + "ns (" + (dt*1e-9) +"s)");
			System.out.println( "No action:    dt=" + dtNoAction + "ns (" + (dtNoAction*1e-9) +"s)");
		}
	}
	
	private static< T, RDD extends JavaRDDLike<T, RDD> > Tuple2< JavaRDDLike<?, ?>, Long > runAction(
			BenchmarkAction< T, RDD > action,
			RDD rdd ) {
		JavaRDDLike<?, ?> out = action.run(rdd);
		long t0 = System.nanoTime();
		out.count();
		long t1 = System.nanoTime();
		long dt = t1 - t0;
		
		return new Tuple2<>( out, dt );
		
	}

}
