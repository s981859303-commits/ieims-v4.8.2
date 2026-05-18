package com.ruoyi.ieims.util;


import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DCB缓存管理器（卫星DCB日缓存 + 接收机DCB估计）
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Component
public class DcbManager {

    private final ConcurrentHashMap<String, Double> satDcbCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> rcvDcbCache = new ConcurrentHashMap<>();

    public double getSatelliteDcb(String satNo, Date date) {
        String key = satNo + "_" + formatDate(date);
        return satDcbCache.getOrDefault(key, 0.0);
    }

    public double getReceiverDcb(String stationId) {
        return rcvDcbCache.getOrDefault(stationId, 0.0);
    }

    public void setSatelliteDcb(String satNo, Date date, double value) {
        String key = satNo + "_" + formatDate(date);
        satDcbCache.put(key, value);
    }

    public void setReceiverDcb(String stationId, double value) {
        rcvDcbCache.put(stationId, value);
    }

    private String formatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(date);
    }
}