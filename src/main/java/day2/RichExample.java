package day2;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Describe:
 *
 * @Author: LuoChao
 * @Project flink-promote
 * @Email luochao40@foxmail.com
 * @Create time: 2021-12-30 21:52
 **/
public class RichExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements(1,2,3)
                .map(new RichMapFunction<Integer, Integer>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("生命周期开始！！！");
                        super.open(parameters);
                    }

                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer* integer;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("生命周期结束！！！");
                    }
                })
                .print();

        env.execute();


    }
}
