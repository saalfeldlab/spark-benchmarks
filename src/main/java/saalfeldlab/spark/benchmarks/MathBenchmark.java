package saalfeldlab.spark.benchmarks;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MathBenchmark
{

	public static void main( final String[] args )
	{
		final SparkConf conf = new SparkConf().setMaster( "local[*]" ).setAppName( "id benchmark" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		final int N = 1000000;
		final ArrayList< Byte > al = new ArrayList<>();
		for ( int i = 0; i < N; ++i )
			al.add( ( byte ) 255 );
		final JavaRDD< Byte > in = sc.parallelize( al ).cache();
		in.count();
		new Evaluate( 10 ).run( sc, in, rdd -> rdd.map( v -> Math.exp( Math.sqrt( v ) ) ) );

		sc.close();
	}

}
