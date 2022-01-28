package com.xb;

/**
 * 根据指标维度汇总后的广告数据
 * @author sxz
 * @version 1.0
 */
public class AdStatDTO {
    public String dimension; //维度
    public Integer  imp ;
    public Integer  click ;
    public Integer  action ;

    public AdStatDTO(String dimension, Integer imp, Integer click, Integer action) {
        this.dimension = dimension;
        this.imp = imp;
        this.click = click;
        this.action = action;
    }

    @Override
    public String toString() {
        return "AdStatDTO{" +
                "dimension='" + dimension + '\'' +
                ", imp=" + imp +
                ", click=" + click +
                ", action=" + action +
                '}';
    }
}
