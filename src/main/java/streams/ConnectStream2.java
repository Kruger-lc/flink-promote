package streams;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.types.ListValue;
import org.apache.flink.util.Collector;

/**
 * @author lc
 * @date 2022/1/10 17:52
 */
public class ConnectStream2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("b", 2),
                        Tuple2.of("a", 2)
                );

        DataStreamSource<Tuple2<String, String>> stream2 = env
                .fromElements(
                        Tuple2.of("a", "a"),
                        Tuple2.of("b", "b"),
                        Tuple2.of("a", "aaa")
                );

        stream1
                .keyBy(r ->r.f0)
                .connect(stream2.keyBy(r ->r.f0))
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>() {

                    private ListState<Tuple2<String, Integer>> listValue1;
                    private ListState<Tuple2<String, String>> listValue2;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listValue1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("list1", Types.TUPLE(Types.STRING,Types.INT)));
                        listValue2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, String>>("list2",Types.TUPLE(Types.STRING,Types.STRING)));
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        listValue1.add(value);
                        for (Tuple2<String, String> tuple2 : listValue2.get()) {
                            out.collect(value +"----"+tuple2);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                        listValue2.add(value);
                        for (Tuple2<String, Integer> tuple2 : listValue1.get()) {
                            out.collect(tuple2+"----"+value );
                        }
                    }
                })
                .print();
        env.execute();
    }
}
