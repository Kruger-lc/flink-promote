package day1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author lc
 * @date 2021/12/29 17:33
 */
public class AvgExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            private Random random = new Random();
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (running){
                    ctx.collect(random.nextInt(10));
                    Thread.sleep(100L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        })
                .map(r -> Tuple2.of(r,1))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(t -> true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
                        return Tuple2.of(integerIntegerTuple2.f0+t1.f0,integerIntegerTuple2.f1+t1.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Double map(Tuple2<Integer, Integer> t1) throws Exception {
                        return (double)t1.f0/t1.f1;
                    }
                })
                .print();
        env.execute();
    }
}
