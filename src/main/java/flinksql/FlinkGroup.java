package flinksql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author lc
 * @date 2022/1/13 10:30
 */
public class FlinkGroup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> stream = env
                .fromElements(
                        Tuple2.of("Mary", "./home"),
                        Tuple2.of("Bob", "./cart"),
                        Tuple2.of("Mary", "./prod?id=1"),
                        Tuple2.of("Liz", "./home")
                );

        //表环境
        EnvironmentSettings build = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, build);

        //数据流 ->动态表
        Table table = tableEnvironment
                .fromDataStream(
                        stream,
                        $("f0").as("user"),
                        $("f1").as("url")
                );

        //注册临时视图
        tableEnvironment.createTemporaryView("clicks",table);

        //sql查询
        Table query = tableEnvironment.sqlQuery(
                "SELECT user, COUNT(url) FROM clicks GROUP BY user"
        );

        // 查询结果转换成数据流
        // 更新日志流（用于查询中有聚合操作的情况）
        tableEnvironment.toChangelogStream(query).print();

        env.execute();

    }
}
