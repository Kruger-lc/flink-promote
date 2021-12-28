package day1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Describe:
 *
 * @Author: LuoChao
 * @Project flink-promote
 * @Email luochao40@foxmail.com
 * @Create time: 2021-12-27 22:24
 **/
public class MapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new SourceFunction<Integer>() {
            private boolean runing = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {

                while (runing){
                    sourceContext.collect(random.nextInt(1000));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                runing = false;
            }
        })
                .filter(t -> t>500)
                .map(t -> Tuple2.of(t,t))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .print();

        env.execute();

    }
}
