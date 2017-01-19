package saalfeldlab.spark.benchmarks.join;

/**
 *
 * @author Philipp Hanslovsky
 *
 */
public interface DataCreator< K, T >
{

	T create( K k );

}
