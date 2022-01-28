package com.xb;

/**
 * 广告指标
 * @author sxz
 * @version 1.0
 */
public class AdStatMetricDTO {
    public Integer  imp ;
    public Integer  click ;
    public Integer  action ;

    public AdStatMetricDTO(Integer imp, Integer click, Integer action) {
        this.imp = imp;
        this.click = click;
        this.action = action;
    }

    @Override
    public String toString() {
        return "AdStatMetricDTO{" +
                "imp=" + imp +
                ", click=" + click +
                ", action=" + action +
                '}';
    }
}
