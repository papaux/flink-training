package me.papaux.flink.tuto;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static me.papaux.flink.tuto.Constants.DATA_DIR;

public class ClickAssignment {


    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        var rawData = env.readTextFile(DATA_DIR + "/ip-data.txt");

        DataStream<DataRecord> data = rawData.map(new DataParser());

        // data.print();

        //a.) total number of clicks on every website in separate file
        data
                .keyBy(v -> v.website)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum("count")
                .writeAsText(DATA_DIR + "/assignment2-number-clicks", FileSystem.WriteMode.OVERWRITE);


        //b.) the website with maximum number of clicks in separate file.
        data
                .keyBy(v -> v.website)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum("count")
                .keyBy(v -> v.website)
                .max("count")
                        .print();
                //.writeAsText(DATA_DIR + "/assignment2-max-clicks", FileSystem.WriteMode.OVERWRITE);

        //c.) the website with minimum number of clicks in separate file.

        //c.) Calculate number of distinct users on every website in separate file.

        //d.) Calculate the average time spent on website by users.

        env.execute("Assignment 2");
    }

    public static class DataParser implements MapFunction<String, DataRecord> {
        @Override
        public DataRecord map(String value) throws Exception {
            // Split the CSV line into individual fields
            String[] fields = value.split(",");

            // Assuming your DataRecord class has appropriate constructors
            return new DataRecord(
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    Long.parseLong(fields[5])
            );
        }
    }

    public static class DataRecord {
        public String userId;
        public String networkName;
        public String userIp;
        public String userCountry;
        public String website;
        public long timeSpent;
        public long count;

        public DataRecord() {

        }

        public DataRecord(String userId, String networkName, String userIp, String userCountry, String website, long timeSpent) {
            this.userId = userId;
            this.networkName = networkName;
            this.userIp = userIp;
            this.userCountry = userCountry;
            this.website = website;
            this.timeSpent = timeSpent;
            this.count = 1;
        }

        @Override
        public String toString() {
            return "DataRecord{" +
                    "userId=" + userId +
                    ", networkName='" + networkName + '\'' +
                    ", userIp='" + userIp + '\'' +
                    ", userCountry='" + userCountry + '\'' +
                    ", website='" + website + '\'' +
                    ", timeSpent=" + timeSpent + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
    // ## user_id,network_name,user_IP,user_country,website, Time spent before next click

}
