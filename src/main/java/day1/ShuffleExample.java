package day1;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lc
 * @date 2021/12/29 17:58
 */
public class ShuffleExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9)
                .setParallelism(1);
//        source
//                .shuffle()//随机分发得到下游分区
//                .print("shuffle").setParallelism(2);
//
//        source
//                .rebalance()//轮询所有数据到下游分区
//                .print("rebalance").setParallelism(2);

        source
                .broadcast()//广播到所有下游分区
                .print("broadcast").setParallelism(2);

        env.execute();
    }
}
