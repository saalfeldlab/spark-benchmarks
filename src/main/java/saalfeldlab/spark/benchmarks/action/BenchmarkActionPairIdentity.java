package saalfeldlab.spark.benchmarks.action;

import org.apache.spark.api.java.JavaPairRDD;

public class BenchmarkActionPairIdentity<K, V> implements
BenchmarkActionPair<K, V> {

	public JavaPairRDD<K, V> run(JavaPairRDD<K, V> rdd) {
		return rdd.mapToPair( t -> t );
	}

}
