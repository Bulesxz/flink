package com.xb;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisFunc {
    public static class AdStatRedisMapper implements RedisMapper<AdStatDTO> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new  RedisCommandDescription(RedisCommand.HSET,"flink_test");
        }

        @Override
        public String getKeyFromData(AdStatDTO o) {
            return o.dimension;
        }

        @Override
        public String getValueFromData(AdStatDTO o) {
            return o.toString() ;
        }
    }
}
