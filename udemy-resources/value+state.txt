package p1;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
 
public class StateDemo 
{
	public static void main(String[] args) throws Exception 
	{
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> data = env.socketTextStream("localhost", 9090);
		
		DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>()
				{
					public Tuple2<Long, String> map(String s)
					{
						String[] words = s.split(",");
						return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
					}
				})
				.keyBy(0)
				.flatMap(new StatefulMap());
		sum.writeAsText("/home/jivesh/state2");

		// execute program
		env.execute("State");
	}
	
	public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> 
	{
		private transient ValueState<Long> sum;            // 2
		private transient ValueState<Long> count;          //  4
		
		public void flatMap(Tuple2<Long, String> input, Collector<Long> out)throws Exception
		{
			Long currCount = count.value();        //   2
			Long currSum = sum.value();             //  4
			
			currCount += 1;
			currSum = currSum + Long.parseLong(input.f1);
			
			count.update(currCount);
			sum.update(currSum);
			
			if (currCount >= 10)
			{
				/* emit sum of last 10 elements */
				out.collect(sum.value());
				/* clear value */
				count.clear();
				sum.clear();
			}
		}		
		public void open(Configuration conf)
		{
			ValueStateDescriptor<Long> descriptor =new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {}), 0L);
	        sum = getRuntimeContext().getState(descriptor);
	        
	        ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>( "count",  TypeInformation.of(new TypeHint<Long>() {}), 0L); 
	        count = getRuntimeContext().getState(descriptor2);
		}
	}
}


