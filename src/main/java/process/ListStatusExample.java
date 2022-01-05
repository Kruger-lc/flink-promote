package process;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.ListValue;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @author lc
 * @date 2022/1/4 14:41
 */
public class ListStatusExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running){
                            ctx.collect(random.nextInt(10));
                            Thread.sleep(100L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy( t ->1)
                .process(new KeyedProcessFunction<Integer, Integer, Double>() {

                    private ListState<Integer> listValue;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listValue = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Types.INT));
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
                        listValue.add(value);
                        Integer sum = 0;
                        Integer count = 0;
                        for (Integer i : listValue.get()) {
                            sum +=i;
                            count +=1;
                        }
                        out.collect((double) sum/count);
                    }
                })
                .print();

        env.execute();
    }
}
