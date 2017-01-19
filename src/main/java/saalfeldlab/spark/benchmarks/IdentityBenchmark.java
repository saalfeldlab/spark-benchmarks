package saalfeldlab.spark.benchmarks;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import saalfeldlab.spark.benchmarks.action.BenchmarkActionIdentity;
import saalfeldlab.spark.benchmarks.action.NoAction;

public class IdentityBenchmark {
	
	public static void main( String[] args ) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("id benchmark");
		JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel("ERROR");
		
		int N = 1000000;
		ArrayList<Byte> al = new ArrayList<>();
		for ( int i = 0; i < N; ++i ) {
			al.add( (byte)0 );
		}
		
		JavaRDD<Byte> rdd = sc.parallelize(al).cache();
		rdd.count();
		new Evaluate( 10 ).run( sc, rdd, new BenchmarkActionIdentity<>() );
		
		sc.close();
	}

}
