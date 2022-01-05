package process;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.MapValue;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * @author lc
 * @date 2022/1/4 15:00
 */
public class MapStatusExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(t ->1)
                .process(new KeyedProcessFunction<Integer, Event, Double>() {

                    private MapState<String,Integer> mapValue;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        mapValue = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("map-status", Types.STRING,Types.INT));
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<Double> out) throws Exception {
                        if (mapValue.contains(value.user)){
                            mapValue.put(value.user, mapValue.get(value.user)+1);
                        }else {
                            mapValue.put(value.user, 1);
                        }

                        int users = 0;
                        int userCounts = 0;

                        for (String key : mapValue.keys()) {
                            users++;
                            userCounts = userCounts + mapValue.get(key);
                        }
                        out.collect((double) userCounts/users);
                    }
                })
                .print();

        env.execute();
    }

    // SourceFunction并行度只能为1
    // 自定义并行化版本的数据源，需要使用ParallelSourceFunction
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
        private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        private Random random = new Random();
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                // collect方法，向下游发送数据
                ctx.collect(
                        new Event(
                                userArr[random.nextInt(userArr.length)],
                                urlArr[random.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }

}
