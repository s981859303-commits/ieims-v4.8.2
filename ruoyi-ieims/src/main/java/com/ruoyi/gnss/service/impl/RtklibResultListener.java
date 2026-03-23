package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.service.IGnssStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Date;

/**
 * RTKLIB 结果监听服务
 * 1. 连接 RTKLIB Output (TCP Client)
 * 2. 解析 NMEA $GPGGA
 * 3. 调用 storageService 接口保存
 */
@Service
public class RtklibResultListener {

    // 监听 RTKLIB 的输出端口 (注意要和 input 端口 5001 区分开)
    private static final String RTKLIB_HOST = "127.0.0.1";
    private static final int RTKLIB_OUTPUT_PORT = 5002;

    @Autowired
    private IGnssStorageService storageService; // 注入接口

    private Thread workerThread;
    private boolean running = true;

    @PostConstruct
    public void start() {
        workerThread = new Thread(this::connectAndRead);
        workerThread.setName("Rtklib-Result-Listener");
        workerThread.start();
    }

    private void connectAndRead() {
        while (running) {
            try (Socket socket = new Socket(RTKLIB_HOST, RTKLIB_OUTPUT_PORT);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                System.out.println("✅ [结果监听] 已连接 RTKLIB Output (" + RTKLIB_OUTPUT_PORT + ")");

                String line;
                while (running && (line = reader.readLine()) != null) {
                    // 只解析 GPGGA (定位数据)
                    if (line.contains("$GPGGA") || line.contains("$GNGGA")) {
                        GnssSolution solution = parseNmeaGga(line);
                        if (solution != null) {
                            // 🔥 核心：调用接口保存数据
                            storageService.saveSolution(solution);
                        }
                    }
                }
            } catch (Exception e) {
                // 连接失败不要疯狂刷屏，稍微等一下重连
                try { Thread.sleep(5000); } catch (InterruptedException ex) {}
            }
        }
    }

    /**
     * 简单的 NMEA GGA 解析器
     * 格式: $GPGGA,时间,纬度,N,经度,E,状态,卫星数,HDOP,海拔...
     */
    private GnssSolution parseNmeaGga(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length < 10) return null;

            GnssSolution sol = new GnssSolution();
            sol.setTime(new Date()); // 这里暂时取系统当前时间，严格来说应该解析 parts[1]

            // 解析经纬度 (NMEA 格式是 ddmm.mmmm，需要转换，这里简化直接存)
            // 真实项目中建议使用 geotools 或手动写转换逻辑
            // 示例转换: 3020.1234 -> 30 + 20.1234/60 = 30.33539
            sol.setLatitude(nmeaToDegree(parts[2]));
            sol.setLongitude(nmeaToDegree(parts[4]));

            sol.setStatus(Integer.parseInt(parts[6])); // 1=单点, 4=固定解, 5=浮点解
            sol.setSatelliteCount(Integer.parseInt(parts[7]));
            sol.setAltitude(Double.parseDouble(parts[9]));

            return sol;
        } catch (Exception e) {
            return null; // 解析失败忽略
        }
    }

    // NMEA (ddmm.mmmm) 转 Degree (dd.dddd)
    private double nmeaToDegree(String nmea) {
        if (nmea == null || nmea.isEmpty()) return 0.0;
        double val = Double.parseDouble(nmea);
        int deg = (int) (val / 100);
        double min = val % 100;
        return deg + (min / 60.0);
    }

    @PreDestroy
    public void stop() {
        running = false;
    }
}