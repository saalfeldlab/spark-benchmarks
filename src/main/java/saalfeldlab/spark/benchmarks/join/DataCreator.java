package saalfeldlab.spark.benchmarks.join;

public interface DataCreator< K, T >
{

	T create( K k );

}
