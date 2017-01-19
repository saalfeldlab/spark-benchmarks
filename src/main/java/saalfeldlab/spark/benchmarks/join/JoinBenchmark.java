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

public class JoinBenchmark {
	
	public static <V> void run(
			JavaSparkContext sc,
			JavaRDD< Integer > in,
			int[][] mapping, 
			DataCreator< Integer, V > creator,
			final int N ) {
		
		JavaPairRDD<Integer, V> big = in.mapToPair( k -> new Tuple2<>( k, creator.create(k) ) ).cache();
		big.count();
		
		Broadcast<int[][]> mappingBC = sc.broadcast(mapping );
		
		JavaPairRDD<Integer, Integer> small = in
				.mapToPair( k -> new Tuple2<>( k, mappingBC.getValue()[k] ) )
				.flatMapToPair( t -> {
					final Iterator<Integer> it = IntStream.of(t._2()).boxed().collect(Collectors.toList()).iterator();
					return new Iterator<Tuple2<Integer, Integer>>() {

						@Override
						public boolean hasNext() {
							return it.hasNext();
						}

						@Override
						public Tuple2<Integer, Integer> next() {
							return new Tuple2<>( t._1(), it.next() );
						}
					};
				} )
				.mapToPair( t -> t.swap() )
				.cache()
				;
		
		small.count();
		
		Evaluate ev = new Evaluate( N );
		
		System.out.println("big.join(small)");
		ev.run(sc, small, new BenchmarkActionJoin<>(big));
		System.out.println();
		
		System.out.println("small.join(big)");
		ev.run(sc, big, new BenchmarkActionJoin<>(small));
		
	}
	
	public static void main( String[] args ) {
		
		int count = Integer.parseInt(args[0]);
		int arraySize = Integer.parseInt(args[1]);
		int N = Integer.parseInt(args[2]);
		String master = args.length > 3 ? args[3] : "local[*]";
		
		DataCreator< Integer, float[] > creator = 
				(Serializable & DataCreator< Integer, float[] > ) i -> new float[ arraySize ];
		int[][] mapping = new int[count][];
		ArrayList< Integer > indices = new ArrayList<>();
		
		for ( int i = 0, cnt = count; i < mapping.length; ++i, cnt -= 2 ) {
			indices.add(i);
			mapping[ i ] = cnt > 0 ? new int[] { cnt, cnt - 1 } : new int[ 0 ];
		}
		
		SparkConf conf = new SparkConf().setMaster(master).setAppName("JoinBenchmark");
		JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel("ERROR");
		
		JavaRDD<Integer> in = sc.parallelize(indices).cache();
		in.count();
		
		run(sc, in, mapping, creator, N);
		
	}

}
