package streams;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author lc
 * @date 2022/1/11 10:17
 */
//实时对账
public class MatchStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> orderStream = env
                .fromElements(
                        Event.of("order-1", "order", 1000L),
                        Event.of("order-2", "order", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        //升序时间戳水位线
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                );

        SingleOutputStreamOperator<Event> weixinStream = env
                .fromElements(
                        Event.of("order-1", "weixin", 30000L),
                        Event.of("order-3", "weixin", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        orderStream
                .keyBy(r ->r.orderId)
                .connect(weixinStream.keyBy(r ->r.orderId))
                .process(new MatchFunction())
                .print();


        env.execute();
    }

    public static class MatchFunction extends CoProcessFunction<Event,Event,String>{

        private ValueState<Event> order;
        private ValueState<Event> winxin;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            order = getRuntimeContext().getState(new ValueStateDescriptor<Event>("order", Types.POJO(Event.class)));
            winxin = getRuntimeContext().getState(new ValueStateDescriptor<Event>("winxin", Types.POJO(Event.class)));
        }

        @Override
        public void processElement1(Event value, Context ctx, Collector<String> out) throws Exception {
            if (winxin.value() == null){
                // 下订单order事件先到达，因为如果weixin事件先到达，那么weixinState就不为空了
                order.update(value);
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10000);
            }else{
                out.collect("订单ID" + value.orderId + "对账成功，winxin事件先到达");
                ctx.timerService().deleteEventTimeTimer(winxin.value().timestamp + 10000);
                winxin.clear();
            }
        }

        @Override
        public void processElement2(Event value, Context ctx, Collector<String> out) throws Exception {
            if (order.value() == null){
                winxin.update(value);
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10000);
            }else{
                out.collect("订单ID" + value.orderId + "对账成功，order事件先到达");
                ctx.timerService().deleteEventTimeTimer(order.value().timestamp + 10000);
                winxin.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (order.value() ==null){
                out.collect("订单ID" + winxin.value().orderId + "对账失败，order事件5s内未到达");
            }
            if (winxin.value() == null){
                out.collect("订单ID" + order.value().orderId + "对账失败，order事件5s内未到达");
            }
        }
    }


    public static class Event {
        public String orderId;
        public String eventType;
        public Long timestamp;

        public Event() {
        }

        public Event(String orderId, String eventType, Long timestamp) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public static Event of(String orderId, String eventType, Long timestamp) {
            return new Event(orderId, eventType, timestamp);
        }

        @Override
        public String toString() {
            return "Event{" +
                    "orderId='" + orderId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
