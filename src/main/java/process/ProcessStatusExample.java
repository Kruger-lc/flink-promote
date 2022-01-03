package process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @author lc
 * @date 2021/12/31 10:41
 */
public class ProcessStatusExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
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
                .keyBy( t ->1)
                //KeyedProcessFunction只能在keyBy
                .process(new KeyedProcessFunction<Integer, Integer, Double>() {

                    //声明一个状态变量作为累加器
                    //状态变量的作用范围是当前key
                    //状态变量是单例（只能针对当前key可以说是单例，当存在多个key时，会存在多个对应的状态变量）
                    private ValueState<Tuple2<Integer,Integer>> valueState;

                    //定时器
                    private ValueState<Long> valuets;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //状态变量的初始化
                        valueState = getRuntimeContext().getState(
                                //状态变量的描述符
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum-count", Types.TUPLE(Types.INT,Types.INT)));

                        valuets = getRuntimeContext().getState(
                                //定时器状态初始化
                                new ValueStateDescriptor<Long>("定时器",Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
                        //累加器操作
                        if (valueState.value() == null){
                            valueState.update(Tuple2.of(value,1));
                        }else {
                            Tuple2<Integer, Integer> tmp = valueState.value();
                            valueState.update(Tuple2.of(tmp.f0+value,tmp.f1+1));
                        }

                        //定时器操作
                        if (valuets.value() == null){
                            long tmpTimer = ctx.timerService().currentProcessingTime() + 10 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(tmpTimer);
                            valuets.update(tmpTimer);
                        }else if(valuets.value() < ctx.timerService().currentProcessingTime()){
                            long tmpTimer = ctx.timerService().currentProcessingTime() + 10 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(tmpTimer);
                            valuets.update(tmpTimer);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (valuets.value()!=null){
                            out.collect((double)valueState.value().f0/valueState.value().f1);
                            System.out.println(new Timestamp(timestamp)+"触发了定时器！！！");
                        }
                    }
                })
                .print();

        env.execute();


    }
}
