package day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Describe:
 *
 * @Author: LuoChao
 * @Project flink-promote
 * @Email luochao40@foxmail.com
 * @Create time: 2021-12-21 23:58
 **/

//针对每个算子的并行度的优先级高于全局并行度
//任务插槽数取算子最大并行度
public class ParallelismExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamContextEnvironment.getExecutionEnvironment();

        //设置全局并行度
        senv.setParallelism(1);

        //设置批数据,并设置并行度
        DataStreamSource<String> streamSource = senv.fromElements("hello world", "spark flink", "hello spark")
                .setParallelism(1);

        //设置输入和输出的泛型
        SingleOutputStreamOperator<SocketExample.WordWithCount> result = streamSource.flatMap(new FlatMapFunction<String, SocketExample.WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<SocketExample.WordWithCount> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(new SocketExample.WordWithCount(s1, 1l));
                }

            }
        })
                .setParallelism(2)

                //分组
                .keyBy(new KeySelector<SocketExample.WordWithCount, String>() {
                    @Override
                    public String getKey(SocketExample.WordWithCount wordWithCount) throws Exception {
                        return wordWithCount.word;
                    }
                })

                //聚合
                //reduce会维护一个累加器
                //累加器和流中元素的类型一样
                .reduce(new ReduceFunction<SocketExample.WordWithCount>() {
                    @Override
                    public SocketExample.WordWithCount reduce(SocketExample.WordWithCount wordWithCount, SocketExample.WordWithCount t1) throws Exception {
                        return new SocketExample.WordWithCount(wordWithCount.word, wordWithCount.count + t1.count);
                    }
                })
                .setParallelism(2)
                ;

        //输出结果
        result.print().setParallelism(1);

        //执行程序
        senv.execute();
    }

    // POJO类
    // 1. 必须是公有类
    // 2. 所有字段必须是public
    // 3. 必须有空构造器
    // 模拟了case class
    public static class WordWithCount {
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
