import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sbenkhaoua on 13/05/15.
 */
public class Main {

    public static void main(String args[]) {
        SparkConf conf = new SparkConf(true)
                .setMaster("local")
                .setAppName("TestSpark")
                .set("spark.executor.memory", "1g");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> pairs = new ArrayList<Tuple2<String, Integer>>();
        Tuple2<String, Integer> e1 = new Tuple2<String, Integer>("Grp1", 5);
        Tuple2<String, Integer> e2 = new Tuple2<String, Integer>("Grp1", 5);
        Tuple2<String, Integer> e3 = new Tuple2<String, Integer>("Grp1", 5);
        Tuple2<String, Integer> e4 = new Tuple2<String, Integer>("Grp2", 5);
        Tuple2<String, Integer> e5 = new Tuple2<String, Integer>("Grp2", 5);
        pairs.add(e1);
        pairs.add(e2);
        pairs.add(e3);
        pairs.add(e4);
        pairs.add(e5);
        //JavaRDD<String> file = context.textFile("test.txt");
        JavaPairRDD<String, Integer> rdd = context.parallelizePairs(pairs).groupBy(new Function<Tuple2<String, Integer>, String>() {
            public String call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._1();
            }
        }).mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {
                return new Tuple2<String, Integer>(stringIterableTuple2._1(), Lists.newArrayList(stringIterableTuple2._2()).size());
            }
        });

        System.out.println("file Result " + rdd.collectAsMap());

    }
}
