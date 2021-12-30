package process;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.sql.Timestamp;

/**
 * Describe:
 *
 * @Author: LuoChao
 * @Project flink-promote
 * @Email luochao40@foxmail.com
 * @Create time: 2021-12-30 22:51
 **/
public class keyed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost",9999)
                .keyBy(r ->1)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

    public static class MyKeyed extends KeyedProcessFunction<Integer,String,String> {


        @Override
        public void processElement(String s, Context context, Collector<String> collector) throws Exception {
            //当前机器时间
            long ts = context.timerService().currentProcessingTime();
            collector.collect("元素"+s+"在"+new Timestamp(ts)+"到达");

            //注册一个10秒之后的定时器
            long tenSecLater = ts + 10 * 1000L;
            //注册的定时器用的是机器时间
            context.timerService().registerProcessingTimeTimer(tenSecLater);
        }

        //定时器也是状态
        //每个key都有自己的定时器
        //对于每个key，在某个时间戳只能注册一个定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect(new Timestamp(timestamp)+"触发了定时器！！！");
        }
    }

}

