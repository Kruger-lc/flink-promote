package day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Describe:
 *
 * @Author: LuoChao
 * @Project flink-promote
 * @Email luochao40@foxmail.com
 * @Create time: 2021-12-28 22:30
 **/
public class FlatmapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> strem = env.fromElements("white", "black", "gray");

        strem.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                if (s.equals("white")){
                    collector.collect(s);
                }else if("black".equals(s)){
                    collector.collect(s);
                    collector.collect(s);
                }
            }
        }).print();

        env.execute();
    }
}
