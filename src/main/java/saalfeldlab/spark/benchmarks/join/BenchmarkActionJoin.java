package saalfeldlab.spark.benchmarks.join;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;

import saalfeldlab.spark.benchmarks.action.BenchmarkActionPair;
import scala.Tuple2;

public class BenchmarkActionJoin< K, V1, V2 > implements BenchmarkActionPair<K, V2 >{
	
	private final JavaPairRDD< K, V1 > joiner;

	public BenchmarkActionJoin(JavaPairRDD<K, V1> joiner) {
		super();
		this.joiner = joiner;
	}

	@Override
	public JavaPairRDD<K, Tuple2<V1, V2>> run(JavaPairRDD<K, V2> joinee) {
		return joiner.join(joinee);
	}

}
