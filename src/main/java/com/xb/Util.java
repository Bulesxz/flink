package com.xb;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
    public static final String DATETIME_CONVENTION = "yyyy-MM-dd";
    // 时间戳转换日期
    public static String timestampToDateStr(Long timestamp){
        SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_CONVENTION);
        String sd = sdf.format(new Date(timestamp)); // 时间戳转换日期
        return sd;
    }
}
