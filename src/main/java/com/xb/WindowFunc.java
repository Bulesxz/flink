package com.xb;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFunc {

    /**
     * 统计计数聚合
     */
    public static class CountAggrate implements AggregateFunction<AdStatEntityDTO, AdStatMetricDTO, AdStatMetricDTO> {

        @Override
        public AdStatMetricDTO createAccumulator() {
            return new AdStatMetricDTO(0,0,0) ;
        }

        @Override
        public AdStatMetricDTO add(AdStatEntityDTO value,AdStatMetricDTO acc) {

            if (value.statType == ConstantAdStatType.STAT_TYPE_IMP) { //曝光
                acc.imp  = acc.imp + value.PV;
            }
            if (value.statType == ConstantAdStatType.STAT_TYPE_CLICK) {//点击
                acc.click  = acc.click + value.PV;
            }
            if (value.statType == ConstantAdStatType.STAT_TYPE_ACTION) {//action
                acc.action  = acc.action + value.PV;
            }
            return acc;
        }

        @Override
        public AdStatMetricDTO getResult(AdStatMetricDTO acc) {
            return acc;
        }

        @Override
        public AdStatMetricDTO merge(AdStatMetricDTO acc1, AdStatMetricDTO acc2) {
            return new AdStatMetricDTO( acc1.imp + acc2.imp, acc1.click + acc2.click,acc1.action + acc2.action);
        }
    }

    /**
     * 统计计数窗口结果
     */
    public static class CountWindow implements  WindowFunction <AdStatMetricDTO, AdStatDTO, String, TimeWindow> {
        @Override
        public void apply (String key, TimeWindow
        timeWindow, Iterable <AdStatMetricDTO> iterable, Collector < AdStatDTO> out) throws Exception {
            for (AdStatMetricDTO t : iterable) {
                AdStatDTO result = new AdStatDTO(key, t.imp,t.click,t.action);
                out.collect(result);
            }
        }
    }
}
