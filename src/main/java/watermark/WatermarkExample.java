package watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author lc
 * @date 2022/1/6 16:14
 */

//关于watermark的理解的两种情况：第一种解释如果是水位线一下的数据直接放弃的话，会存在丢未计算窗口的数据，所以关于水位线只能是第二种解释
//                           或者说根本不存在水位线一下的数据直接放弃，而是结束时间低于水位线的窗口的数据放弃，这样可以解释第一情况
//1.水位线 = 已经到来的最大事件时间 - 延迟计算时间 -1
 //2.水位线 = 上个窗口结束时间 + 延迟时间 -1
public class WatermarkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .map(r -> Tuple2.of(r.split(" ")[0],Long.parseLong(r.split(" ")[1])*1000))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                //分配时间戳和水印
                .assignTimestampsAndWatermarks(
                        //设置水位线策略：设置最大延迟时间，设置有界无序的延迟时间
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        //设置水印字段
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                return stringLongTuple2.f1;
                            }
                        })
                )
                .keyBy(t ->t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = elements.spliterator().getExactSizeIfKnown();//返回迭代器中的元素数量
                        out.collect(key+"-----"+new Timestamp(start)+"----"+new Timestamp(end)+"-----"+count);
                    }
                })
                .print();


        env.execute();
    }
}
