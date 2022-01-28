package com.xb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("hello flink");

        //0. 读取配置文件
        Properties prop = new Properties();
        prop.load(App.class.getResourceAsStream("/conf.properties"));

        //1. 获取flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 设置kafka 数据源

        String topic = prop.getProperty("kafka.topic");
        String bootstrapServers = prop.getProperty("kafka.bootstrapServers");
        String groupId = prop.getProperty("kafka.groupId");
        KafkaSource<String> source = KafkaSource
                .<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(groupId)
                .setTopics(Arrays.asList(topic))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
        final DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //3. 过滤，只保留需要的stat数据
        final SingleOutputStreamOperator<String> statStream = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                String statName = JSON.parseObject(line).getString("statName");
                String err = JSON.parseObject(line).getJSONObject("data").getString("err");
                String crid = JSON.parseObject(line).getJSONObject("data").getString("crid");
                String statType = JSON.parseObject(line).getJSONObject("data").getString("stat_type");

                if ( !statName.equals("stat") ) return false; //stat 上报数据
                if ( err !=  null ) return false; //正确上报
                if ( !(crid.length() >=7 ) ) return false; //创意上报
                if (!(statType.equals("1") || statType.equals("2") || statType.equals("5"))) return false; //曝光，点击，action
                return true;
            }
        });

        //4. 源数据转化成AdStatEntityDTO结构
        final SingleOutputStreamOperator<AdStatEntityDTO> entityStresm = statStream.map(new MapFunction<String, AdStatEntityDTO>() {
            @Override
            public AdStatEntityDTO map(String line) throws Exception {
                JSONObject jsonObject = JSON.parseObject(line);
                final JSONObject data = jsonObject.getObject("data", JSONObject.class);
                Long ts = data.getLong("ts")*1000L; //毫秒
                String ds =  Util.timestampToDateStr(Long.valueOf(ts));
                String orderID = data.getString("order_id");
                String crid = data.getString("crid");
                String app_id = data.getString("app_id");
                String position_id = data.getString("position_id");
                Integer stat_type = data.getInteger("stat_type");
                AdStatEntityDTO entity = new AdStatEntityDTO(ts,ds,orderID, crid, position_id, app_id, stat_type,1);
                return entity;
            }
        });

        entityStresm.print();

         // 5. 设置水印策略
        final SingleOutputStreamOperator<AdStatEntityDTO> adStatEntitySingleOutputStreamOperator = entityStresm.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<AdStatEntityDTO>forBoundedOutOfOrderness(Duration.ofSeconds(60)) //最大延迟时间
                        .withIdleness(Duration.ofMinutes(1)) //处理空闲数据源等待时间，避免部分分区没有达到watermark
                        .withTimestampAssigner((event, timestamp) -> event.ts)); //event时间的获取


        // 6. 根据统计维度分组
        final KeyedStream<AdStatEntityDTO, String> adStatEntityObjectKeyedStream = adStatEntitySingleOutputStreamOperator.keyBy(value -> value.genPositionCridKey());
//        adStatEntityObjectKeyedStream.print();

        //7. 设置窗口， 滑动窗口 大小为3天， 60滑动一次
        final WindowedStream<AdStatEntityDTO, String, TimeWindow> window = adStatEntityObjectKeyedStream.window(SlidingEventTimeWindows.of( Time.days(3), Time.seconds (60))); //滑动 event-time 窗口

        // 8. 聚合窗口数据
        SingleOutputStreamOperator<AdStatDTO> aggregate = window.aggregate(new WindowFunc.CountAggrate(), new WindowFunc.CountWindow());
        aggregate.print();


        // 9. 设置sink, 写入redis
        String redisHost = prop.getProperty("redis.host");
        Integer redisPort =  Integer.parseInt( prop.getProperty("redis.port"));
        String redisPass = prop.getProperty("redis.pass");

        final FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(redisPort)
                .setPassword(redisPass)
                .build();

        final RedisSink<AdStatDTO> adStatDTORedisSink = new RedisSink<>(redisConf, new RedisFunc.AdStatRedisMapper());
        aggregate.addSink(adStatDTORedisSink);


        // 10. 执行任务
        env.execute("flink");
    }

}
