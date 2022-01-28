package com.xb;

/**
 * 从日志清洗出的广告日志
 * @author sxz
 * @version 1.0
 */
public class AdStatEntityDTO {
    public Long ts;
    public String ds;
    public String orderId;
    public String crID;
    public String positionID;
    public String appID;
    public Integer  PV ;
    public int statType;

    public AdStatEntityDTO(Long ts, String ds, String orderId, String crID, String positionID, String appID, int statType, int PV) {
        this.ts = ts;
        this.ds = ds;
        this.orderId = orderId;
        this.crID = crID;
        this.positionID = positionID;
        this.appID = appID;
        this.statType = statType;
        this.PV = PV;
    }
    @Override
    public String toString() {
        return "AdStatEntityDTO{" +
                "ds='" + ds + '\'' +
                ", ts='" + ts + '\'' +
                ", orderId='" + orderId + '\'' +
                ", crID='" + crID + '\'' +
                ", positionID='" + positionID + '\'' +
                ", appID='" + appID + '\'' +
                ", PV='" + PV + '\'' +
                ", statType=" + statType +
                '}';
    }

    public  String genPositionCridKey() {
        return  this.positionID  + "_" + this.crID;
    }
}
