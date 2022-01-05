package window;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

/**
 * @author lc
 * @date 2022/1/5 17:08
 */
public class ProcessRealizationWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r ->r.user)
                .process(new FakeWindow())
                .print();

        env.execute();
    }


    public static class FakeWindow extends KeyedProcessFunction<String,Event,String>{

        private MapState<Long,Integer> mapState;

        private Long windowSize = 5000L;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState=getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("map-name", Types.LONG,Types.INT));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            long currTime = ctx.timerService().currentProcessingTime();
            long currWindowstartTime =currTime - currTime % windowSize;//获取当前窗口的开始时间
            long currWindowendTime =currWindowstartTime + windowSize-1;//当前窗口的结束时间

            if (mapState.contains(currWindowstartTime)) {
                mapState.put(currWindowstartTime,mapState.get(currWindowstartTime)+1);
            }else{
                mapState.put(currWindowstartTime,1);
            }

            //添加定时器
            ctx.timerService().registerProcessingTimeTimer(currWindowendTime-1L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long windowend = timestamp +1L ;
            long windowstart = windowend - windowSize;
            long count = mapState.get(windowstart);
            out.collect(ctx.getCurrentKey()+"-----"+new Timestamp(windowstart)+"----"+new Timestamp(windowend)+"-----"+count);
        }
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
