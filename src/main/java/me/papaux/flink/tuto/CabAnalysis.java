package me.papaux.flink.tuto;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static me.papaux.flink.tuto.Constants.DATA_DIR;

public class CabAnalysis {

    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        var data = env.readTextFile(DATA_DIR + "/cab_flink.txt");

        var mapped = data.map(new Splitter());

        // 1.) Popular destination.  | Where more number of people reach.
        mapped
                .keyBy(v -> v.f6)
                .sum(7)
                .keyBy(v -> v.f6)
                .max(7)
                .writeAsText(DATA_DIR + "/cab_dest", FileSystem.WriteMode.OVERWRITE);

        // 2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
        mapped
                .keyBy(v -> v.f5)
                .reduce((v, w) -> new Tuple9<>(v.f0, v.f1, v.f2, v.f3, v.f4, v.f5, v.f6, v.f7 + w.f7, v.f8 + w.f8))

                .map(new MapFunction<>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple9<String, String, String, String, String, String, String, Double, Integer> value) throws Exception {
                        return new Tuple2<String, Double>(value.f6, value.f7 * 1.0 / value.f8);
                    }
                })

                .writeAsText(DATA_DIR + "/cab_avg_pickup", FileSystem.WriteMode.OVERWRITE);


        // 3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made
        mapped
                .keyBy(v -> v.f3)
                .reduce((v, w) -> new Tuple9<>(v.f0, v.f1, v.f2, v.f3, v.f4, v.f5, v.f6, v.f7 + w.f7, v.f8 + w.f8))

                .map(new MapFunction<>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple9<String, String, String, String, String, String, String, Double, Integer> value) throws Exception {
                        return new Tuple2<String, Double>(value.f3, value.f7 * 1.0 / value.f8);
                    }
                })

                .writeAsText(DATA_DIR + "/cab_avg_driver", FileSystem.WriteMode.OVERWRITE);

        // execute program
        env.execute("Assignment1");
    }

    /**
     * 0         1                 2         3                4                 5                6           7
     * # cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count
     */
    public static class Splitter implements
            MapFunction<String, Tuple9<String, String, String, String, String, String, String, Double, Integer>> {
        public Tuple9<String, String, String, String, String, String, String, Double, Integer> map(String value) {
            // id_4213,PB7526,Sedan,Wanda,yes,Sector 19,Sector 10,5
            String[] t = value.split(",");
            return new Tuple9<>(t[0], t[1], t[2], t[3], t[4], t[5], t[6], doubleOrZero(t[7]), 1);
        }

        private Double doubleOrZero(String input) {
            try {
                return Double.parseDouble(input);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }
}
