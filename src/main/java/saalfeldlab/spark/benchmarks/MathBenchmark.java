package saalfeldlab.spark.benchmarks;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import saalfeldlab.spark.benchmarks.action.BenchmarkActionIdentity;
import saalfeldlab.spark.benchmarks.action.NoAction;

public class MathBenchmark {
	
	public static void main( String[] args ) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("id benchmark");
		JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel("ERROR");
		
		int N = 1000000;
		ArrayList<Byte> al = new ArrayList<>();
		for ( int i = 0; i < N; ++i ) {
			al.add( (byte)255 );
		}
		JavaRDD<Byte> in = sc.parallelize(al).cache();
		in.count();
		new Evaluate( 10 ).run( sc, in, rdd -> rdd.map( v -> Math.exp( Math.sqrt( v ) ) ) );
		
		sc.close();
	}

}
