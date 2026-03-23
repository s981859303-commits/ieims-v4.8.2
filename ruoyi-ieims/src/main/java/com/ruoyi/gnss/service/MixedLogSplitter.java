package com.ruoyi.gnss.service; // 确保包名正确

import cn.hutool.json.JSONArray;
import com.ruoyi.common.json.JSONObject;
import com.ruoyi.gnss.service.GnssDataListener;
import com.ruoyi.gnss.service.MixedLogSplitter;
import com.ruoyi.gnss.service.RtklibNative;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class MixedLogSplitter {
    private Map<Integer, RtklibNative.JavaObs> epochCache = new HashMap<>();
    private long lastEpochTime = 0;
    // 注入监听器列表（支持多个接收者，如推送到RTKLIB的服务、存数据库的服务）
    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

    // 1MB 缓冲区
    private ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    // 注入你的 MQTT 服务 (类名以你实际的为准)
    @Autowired
    private GnssMqttService mqttService;

    // 定义用于发送给 TDengine 的专用 MQTT 主题
    private static final String TDENGINE_MQTT_TOPIC = "tdengine/rtcm/data";

    /**
     * 入口方法：接收网络/串口传来的原始字节流
     */
    public synchronized void pushData(byte[] newBytes) {
        //  新增日志
        System.out.println("📥 [Debug] MixedLogSplitter 收到数据: " + newBytes.length + " 字节");
        try {
            if (buffer.remaining() < newBytes.length) {
                buffer.compact();
                if (buffer.remaining() < newBytes.length) buffer.clear();
            }
            buffer.put(newBytes);
            buffer.flip();
            parseAndDispatch(); // 解析分包
            buffer.compact();
        } catch (Exception e) {
            System.err.println("数据解析异常: " + e.getMessage());
            e.printStackTrace();
            // 发生异常时，如果 buffer 还有数据，不要彻底 clear，而是把 limit 恢复，准备下一次 compact
            // 这里最安全的做法是不做 clear，依靠下方的 buffer.compact() 自动将剩余未读数据保留
        }
    }

    private void parseAndDispatch() {
        while (buffer.hasRemaining()) {
            buffer.mark();
            byte b = buffer.get();

            if (b == 0x24) { // NMEA ($)
                if (!tryExtractNmea()) {
                    buffer.reset();
                    break;
                }
            } else if ((b & 0xFF) == 0xD3) { // RTCM (0xD3)
                if (!tryExtractRtcm()) {
                    break; // 数据不足，跳出循环等待下一次 pushData
                }
            }
        }
    }

    private boolean tryExtractNmea() {
        int startPos = buffer.position() - 1;
        for (int i = buffer.position(); i < buffer.limit(); i++) {
            if (i - startPos > 1000) { buffer.position(i); return true; }
            if (buffer.get(i) == 0x0A) {
                int len = i - startPos + 1;
                byte[] lineBytes = new byte[len];
                buffer.position(startPos);
                buffer.get(lineBytes);

                String nmea = new String(lineBytes, StandardCharsets.US_ASCII).trim();
                notifyNmea(nmea);

                buffer.position(i + 1);
                return true;
            }
        }
        return false;
    }

    private boolean tryExtractRtcm() {
        if (buffer.remaining() < 2) { buffer.reset(); return false; }

        int p = buffer.position();
        // 解析 RTCM 长度 (前3个字节包含长度信息，RTCM3 长度在 Byte1 的后6位和 Byte2)
        int length = ((buffer.get(p) & 0x03) << 8) | (buffer.get(p + 1) & 0xFF);

        if (length > 1200 || length == 0) {
            buffer.reset(); buffer.get(); return true; // 假头，跳过
        }

        // RTCM 总长度 = Header(3) + Body(length) + CRC(3)
        // 但你的代码里写的是 length + 6，这取决于 length 具体指代什么。
        // 标准 RTCM3 中，length 字段仅指 Data Message 的长度，不含 Header(3) 和 CRC(3)。
        // 所以总长度通常是 length + 6。
        int totalLen = length + 6;

        if (buffer.remaining() < totalLen - 1) {
            buffer.reset(); return false; // 数据不够，等待更多数据
        }

        byte[] rtcmFrame = new byte[totalLen];
        buffer.position(buffer.position() - 1); // 回退到 0xD3 的位置
        buffer.get(rtcmFrame);

        //  这里提取出了完整的 RTCM 帧，直接分发
        notifyRtcm(rtcmFrame);

        return true;
    }

    // --- 内部通知方法 ---

    private void notifyNmea(String nmea) {
        // 分发给原有监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try { listener.onNmeaReceived(nmea); } catch (Exception e) { e.printStackTrace(); }
            }
        }

        // 🔥 新增：如果是 GGA 数据，解析并推送到 TDengine
        if (nmea.startsWith("$GPGGA") || nmea.startsWith("$GNGGA") || nmea.startsWith("$BDGGA")) {
            parseAndSendGgaToTDengine(nmea);
        }
    }

    // ---------------- 新增的解析与发送方法 ----------------
    private void parseAndSendGgaToTDengine(String ggaLine) {
        try {
            String[] parts = ggaLine.split(",", -1);
            // 校验 GGA 格式基本完整且经纬度不为空
            if (parts.length < 10 || parts[2].isEmpty() || parts[4].isEmpty()) return;

            // NMEA 的经纬度是度分格式 (ddmm.mmmm)，需要转成十进制度 (dd.dddd)
            double lat = nmeaToDecimal(parts[2], parts[3]);
            double lon = nmeaToDecimal(parts[4], parts[5]);

            int satUsed = parts[7].isEmpty() ? 0 : Integer.parseInt(parts[7]);
            double hdop = parts[8].isEmpty() ? 0.0 : Double.parseDouble(parts[8]);
            double alt = parts[9].isEmpty() ? 0.0 : Double.parseDouble(parts[9]);

            // 组装 JSON
            JSONObject metric = new JSONObject();
            metric.put("name", "st_receiver_status"); // ⭐️ 对应超级表 st_receiver_status
            metric.put("timestamp", System.currentTimeMillis());

            JSONObject tags = new JSONObject();
            tags.put("station_id", "8900_1"); // 测试站点 ID
            metric.put("tags", tags);

            JSONObject fields = new JSONObject();
            fields.put("lat", lat);
            fields.put("lon", lon);
            fields.put("alt", alt);
            fields.put("sat_used", satUsed);
            fields.put("hdop", hdop);
            metric.put("fields", fields);

            JSONArray metricsArray = new JSONArray();
            metricsArray.add(metric);

            JSONObject finalPayload = new JSONObject();
            finalPayload.put("metrics", metricsArray);

            // 发送给 MQTT
            if (mqttService != null) {
                mqttService.publish(TDENGINE_MQTT_TOPIC, finalPayload.toString());
                System.out.println("✅ [NMEA] 成功推送接收机坐标到 TDengine: Lat=" + lat + ", Lon=" + lon);
            }

        } catch (Exception e) {
            System.err.println("⚠️ GGA 解析失败: " + e.getMessage());
        }
    }

    // ---------------- 辅助方法：经纬度度分转十进制 ----------------
    private double nmeaToDecimal(String nmeaPos, String dir) {
        if (nmeaPos == null || nmeaPos.isEmpty()) return 0.0;
        int dotIndex = nmeaPos.indexOf(".");
        if (dotIndex < 2) return 0.0;

        // 提取度和分
        int deg = Integer.parseInt(nmeaPos.substring(0, dotIndex - 2));
        double min = Double.parseDouble(nmeaPos.substring(dotIndex - 2));
        double decimal = deg + (min / 60.0);

        // 南半球和西半球取负
        if ("S".equalsIgnoreCase(dir) || "W".equalsIgnoreCase(dir)) {
            decimal = -decimal;
        }
        return decimal;
    }

    private void notifyRtcm(byte[] data) {
        //  新增日志
        System.out.println("📦 [Debug] 识别到完整 RTCM 帧，长度: " + data.length + "，准备交给 DLL 解算...");

        // 1. 🔥 核心修改：在此处直接调用 JNA 解算并打印到控制台
        // 只要有 RTCM 数据包进来，就尝试解算，不依赖监听器
        decodeAndPrintRtcm(data);

        // 2. 分发给其他业务监听器 (如推送到 RTKLIB 端口)
        if (listeners == null || listeners.isEmpty()) {
            // System.err.println("⚠️ 无监听器，仅控制台打印解码结果");
            return;
        }

        for (GnssDataListener listener : listeners) {
            try {
                listener.onRtcmReceived(data);
            } catch (Exception e) {
                System.err.println("❌ 推送异常: " + e.getMessage());
            }
        }
    }

    /**
     *  JNA 调用逻辑：解算 RTCM 并输出到控制台
     */
    private void decodeAndPrintRtcm(byte[] rtcmData) {
        try {
            // 1. 准备内存空间：一次最多接收 64 颗卫星的数据
            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(64);

            // 2. 调用 C 语言 DLL 函数
            int count = RtklibNative.INSTANCE.parse_rtcm_frame(rtcmData, rtcmData.length, obsRef, 64);

            // 3. 如果 count > 0，说明是观测值帧 (1074/1127 等)，打印数据
            if (count > 0) {
                // 随便取第一颗卫星的时间戳（实际应该从C返回time，这里用系统时间模拟检测）
                // RTCM 帧没有直接暴露时间，我们通过“连续性”来判断
                // 简单策略：如果收到新数据，先存缓存。
                // 实际上，你需要一个触发机制。这里为了简化，我们打印"当前缓存池状态"。

                updateEpochCache(obsArray, count);
            }
        } catch (UnsatisfiedLinkError e) {
            System.err.println("❌ 严重错误：找不到 rtklib_bridge.dll 文件！请确保它在项目根目录下。");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 辅助工具：将 RTKLIB 的 int 卫星号转为可读字符串 (Demo5 版本)
     */
    private String getSatName(int satId) {
        // 1. GPS: 1 ~ 32
        if (satId >= 1 && satId <= 32) {
            return String.format("G%02d", satId);
        }

        // 2. SBAS: 33 ~ 55 (通常是包含在 GPS 里的增强系统)
        if (satId >= 33 && satId <= 55) {
            return String.format("S%02d", satId);
        }

        // 3. GLONASS: 56 ~ 85 (RTKLIB 默认偏移量，Demo5 可能不同，这里是通用估算)
        // 有些版本是 33~56，如果看到 R 开头的卫星号不对，可以调整这个范围
        if (satId >= 56 && satId <= 85) {
            return String.format("R%02d", satId - 55);
        }

        // 4. Galileo: 86 ~ 122 (估算范围)
        if (satId >= 86 && satId <= 122) {
            return String.format("E%02d", satId - 85);
        }

        // 5. BeiDou: 140 ~ 180 (Demo5 b34 通常从 141 开始)
        // SAT144 -> C04, SAT145 -> C05
        if (satId >= 140 && satId <= 190) {
            return String.format("C%02d", satId - 140);
        }

        // 其他未知卫星
        return "SAT" + satId;
    }

    // 定义一个简单的内部类来存数据，防止 JNA 内存释放导致乱码
    class SatData {
        int sat;
        Double p1, p2, l1, l2, s1;
        String satName;
        String c1, c2; // 新增信号类型
    }

    // 缓存池：Key是卫星号，Value是数据
    private Map<Integer, SatData> satCache = new HashMap<>();
    private long lastPrintTime = System.currentTimeMillis();

    /**
     *  核心逻辑：聚合 + 定时打印
     * 这里不再是收到一个包打印一次，而是把数据存进 Map，每秒打印一次完整的列表
     */
    private void updateEpochCache(RtklibNative.JavaObs[] newObs, int count) {
        // 1. 把新收到的卫星存入缓存 Map (去重)
        for (int i = 0; i < count; i++) {
            RtklibNative.JavaObs obs = newObs[i];

            // 过滤无效数据
            if (obs.P[0] == 0.0) continue;

            SatData data = new SatData();
            data.sat = obs.sat;
            data.satName = new String(obs.id).trim();

            // 解析信号代码 (1C, 2W...)
            data.p1 = obs.P[0];
            data.l1 = obs.L[0];

            //  P1/L1: 必定存在 (因为上面过滤了)
            data.p1 = obs.P[0];
            data.l1 = obs.L[0];

//            // 智能获取 P2/L2 (优先取 P2，如果为0则取 P3)
//            if (obs.P[1] != 0) {
//                data.p2 = obs.P[1];
//                data.l2 = obs.L[1];
//                // 如果用到 P2，C2 就是 code[1] (即 8-15 位)
//            } else {
//                data.p2 = obs.P[2];
//                data.l2 = obs.L[2];
//                // 如果用到 P3，那 C2 应该显示 code[2] (即 16-23 位)
//                data.c2 = new String(obs.code, 16, 8).trim();
//            }

            //  P2/L2: 严格读取 index 1，不再查找 P[2] (index 2)
            // 如果 obs.P[1] 是 0.0，我们就存为 null，表示该频点无数据
            data.p2 = (obs.P[1] != 0.0) ? obs.P[1] : null;
            data.l2 = (obs.L[1] != 0.0) ? obs.L[1] : null;

            // SNR 修正：如果读数很小(比如12)，通常是因为单位问题，这里简单做个显示处理
            // RTKLIB 内部 SNR 单位通常是 0.25 dBHz，但这里我们直接打印原始值观察
            // 如果你发现数值在 30-50 之间就是正常的 dBHz
            //修改为真实信噪比值显示
            // S1 (信噪比) - RTKNAVI 原图显示的是 dBHz (例如 46.0)
            // 我们的 DLL 已经传回了 float snr，如果是 11.0，需要 * 4
            data.s1 = (obs.snr[0] > 0) ? obs.snr[0] * 4.0 : 0;

            // 存入 Map (如果卫星号相同，覆盖旧数据)
            satCache.put(data.sat, data);
        }

        // 2. 检查时间，如果距离上次打印超过 900ms (接近1秒)，就打印一次完整列表
        long now = System.currentTimeMillis();
        if (now - lastPrintTime > 950) {
            printCacheAndClear();
            lastPrintTime = now;
        }
    }

    /**
     * 🖨️ 打印并推送到 MQTT (TDengine 测试数据库格式)
     */
    /**
     * 🖨️ 打印缓存数据并推送到 MQTT (TDengine Schemaless 格式)
     */
    /**
     * 🖨️ 打印缓存数据并推送到 MQTT (TDengine Schemaless 格式)
     */
    private void printCacheAndClear() {
        if (satCache.isEmpty()) return;

        System.out.println("=======================================================================================================");
        System.out.println("🚀 RTCM 实时数据 | 时间: " + new java.util.Date() + " | 总星数: " + satCache.size());
        System.out.printf("%-6s %-4s %-4s %-8s %-14s %-14s %-14s %-14s\n",
                "SAT", "C1", "C2", "S1(dBHz)", "P1(m)", "P2(m)", "L1(cycle)", "L2(cycle)");
        System.out.println("-------------------------------------------------------------------------------------------------------");

        // 🔥 1. 准备组装 TDengine JSON 数组
        JSONArray metricsArray = new JSONArray();
        long currentTimestamp = System.currentTimeMillis();

        satCache.values().stream()
                .sorted((a, b) -> Integer.compare(a.sat, b.sat))
                .forEach(d -> {
                    // 控制台打印 (保持原有 UI)
                    System.out.printf("%-6s %-4s %-4s %-8s %-14s %-14s %-14s %-14s\n",
                            d.satName, d.c1, d.c2,
                            formatVal(d.s1), formatVal(d.p1), formatVal(d.p2),
                            formatVal(d.l1), formatVal(d.l2));

                    // 🔥 2. 构建单颗卫星的 JSON 数据点
                    JSONObject metric = new JSONObject();
                    metric.put("name", "rtcm_data"); // 对应 TDengine 的超级表名称
                    metric.put("timestamp", currentTimestamp);

                    // Tags: 标签数据 (用于建表和快速检索)
                    JSONObject tags = new JSONObject();
                    tags.put("sat_name", d.satName);
                    metric.put("tags", tags);

                    // Fields: 实际观测值数据 (过滤掉 null 值)
                    JSONObject fields = new JSONObject();
                    if (d.c1 != null && !d.c1.isEmpty()) fields.put("c1", d.c1);
                    if (d.c2 != null && !d.c2.isEmpty()) fields.put("c2", d.c2);
                    if (d.s1 != null) fields.put("s1", d.s1);
                    if (d.p1 != null) fields.put("p1", d.p1);
                    if (d.p2 != null) fields.put("p2", d.p2);
                    if (d.l1 != null) fields.put("l1", d.l1);
                    if (d.l2 != null) fields.put("l2", d.l2);

                    metric.put("fields", fields);
                    metricsArray.add(metric);
                });

        System.out.println("=======================================================================================================\n");

        // 🔥 3. 将组装好的 JSON 发送到 MQTT
        try {
            if (!metricsArray.isEmpty() && mqttService != null) {
                JSONObject finalPayload = new JSONObject();
                finalPayload.put("metrics", metricsArray);

                // 执行发布
                mqttService.publish(TDENGINE_MQTT_TOPIC, finalPayload.toString());
            }
        } catch (Exception e) {
            System.err.println("❌ 组装或发送 MQTT 数据失败: " + e.getMessage());
        }

        // 清空缓存
        satCache.clear();
    }

    // 辅助方法：处理 Double 转 String
    private String formatVal(Double val) {
        if (val == null || val == 0.0) {
            return "null"; // 或者你想要显示为空白，就返回 ""
        }
        // 如果是比较大的数(伪距)，保留3位小数；如果是信噪比，也可以保留1位
        // 这里统一保留3位，你可以根据需要微调
        return String.format(java.util.Locale.US, "%.3f", val);
    }
}



