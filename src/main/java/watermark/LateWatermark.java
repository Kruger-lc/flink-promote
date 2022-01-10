package watermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lc
 * @date 2022/1/10 9:16
 */
public class LateWatermark {

    private static OutputTag<String> lateElement = new OutputTag<String>("later"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> process = env
                .addSource(new SourceFunction<Tuple2<String, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                        // 指定时间戳发送数据
                        ctx.collectWithTimestamp(Tuple2.of("hello world", 1000L), 1000L);
                        //发送水位线
                        ctx.emitWatermark(new Watermark(999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello flink", 2000L), 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello late", 1000L), 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            ctx.output(lateElement, "迟到数据:" + value.f0);
                        } else {
                            out.collect("正常数据：" + value);
                        }
                    }
                });

        process.print("主输出：");
        process.getSideOutput(lateElement).print("侧输出：");


        env.execute();


    }
}
