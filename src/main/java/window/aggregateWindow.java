package window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import process.WindowProcessExample;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * @author lc
 * @date 2022/1/5 16:43
 */
public class aggregateWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r ->r.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Integer, Integer>() {

                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(Event event, Integer integer) {
                                   return integer + 1;
                               }

                               @Override
                               public Integer getResult(Integer integer) {
                                   return integer;
                               }

                               @Override
                               public Integer merge(Integer integer, Integer acc1) {
                                   return null;
                               }
                           }
                           //输入的泛型是增量输出的泛型
                        , new ProcessWindowFunction<Integer, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                long count = elements.iterator().next();//返回迭代器中的元素数量
                                out.collect(key+"-----"+new Timestamp(start)+"----"+new Timestamp(end)+"-----"+count);
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
