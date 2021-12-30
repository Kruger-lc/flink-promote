package day2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * Describe:
 *
 * @Author: LuoChao
 * @Project flink-promote
 * @Email luochao40@foxmail.com
 * @Create time: 2021-12-30 21:59
 **/
public class RichSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new RichParallelSourceFunction<Integer>() {

                    @Override
                    public void close() throws Exception {
                        System.out.println(getRuntimeContext().getIndexOfThisSubtask()+"生命周期关闭！！！！");
                        super.close();
                    }

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(2);

        env.execute();

    }
}
