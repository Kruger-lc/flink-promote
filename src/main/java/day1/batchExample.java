package day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author lc
 * @date 2021/12/21 17:47
 */
//批处理数据
public class batchExample {

    public static void main(String[] args) throws Exception {
        //流环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度:分区
        senv.setParallelism(1);

        //获取数据源
        //DataStreamSource<String> stream = senv.socketTextStream("localhost", 9999);

        //获取批数据源
        DataStreamSource<String> streamSource = senv.fromElements("hello world", "hello world", "kkk world","world");

        //设置输入和输出的泛型
        SingleOutputStreamOperator<socketExample.WordWithCount> result = streamSource.flatMap(new FlatMapFunction<String, socketExample.WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<socketExample.WordWithCount> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(new socketExample.WordWithCount(s1, 1l));
                }

            }
        })
                //分组
                .keyBy(new KeySelector<socketExample.WordWithCount, String>() {
                    @Override
                    public String getKey(socketExample.WordWithCount wordWithCount) throws Exception {
                        return wordWithCount.word;
                    }
                })

                //聚合
                //reduce会维护一个累加器
                //累加器和流中元素的类型一样
                .reduce(new ReduceFunction<socketExample.WordWithCount>() {
                    @Override
                    public socketExample.WordWithCount reduce(socketExample.WordWithCount wordWithCount, socketExample.WordWithCount t1) throws Exception {
                        return new socketExample.WordWithCount(wordWithCount.word, wordWithCount.count + t1.count);
                    }
                });

        //输出结果
        result.print();

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
