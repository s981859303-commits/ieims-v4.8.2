package com.ruoyi.gnss;

import com.ruoyi.gnss.service.RtklibNative;
import com.sun.jna.Pointer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * 实时 GNSS 数据底层监听器 (纯 Java 独立运行)
 * 直接连接 MQTT，拦截二进制流，调用 DLL 解算 RTCM，并实时打印 NMEA 以及双频观测值(P1/L1/P2/L2)。
 */
public class RealTimeGnssMonitor {

    // MQTT 配置
    private static final String BROKER_URL = "tcp://broker.emqx.io:1883";
    private static final String TOPIC = "/ieims/gnss/data/+";

    private static Pointer rtklibContext = null;
    private static final int MAX_OBS = 64;

    // RTCM3 CRC24Q 校验表 (防止伪造的 0xD3 帧导致解析乱码)
    private static final int[] CRC24Q_TABLE = new int[256];
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 16;
            for (int j = 0; j < 8; j++) {
                crc <<= 1;
                if ((crc & 0x1000000) != 0) crc ^= 0x1864CFB;
            }
            CRC24Q_TABLE[i] = crc & 0xFFFFFF;
        }
    }

    public static void main(String[] args) {
        System.out.println("==================================================");
        System.out.println("🚀 启动实时 GNSS 真实数据流监听器 (含双频数据 P1/L1/P2/L2)");
        System.out.println("==================================================");

        // 1. 初始化 DLL 上下文 (直接调用 JNA)
        try {
            System.out.println("正在加载 rtklib_bridge.dll...");
            rtklibContext = RtklibNative.INSTANCE.rtklib_create_context("live_test_station");
            System.out.println("✅ DLL 加载成功，Context ID: " + rtklibContext);
        } catch (Exception e) {
            System.err.println("❌ DLL 加载失败，请确保环境配置正确: " + e.getMessage());
            return;
        }

        // 2. 连接 MQTT 直接截取底层数据
        try {
            String clientId = "live-monitor-" + UUID.randomUUID().toString().substring(0, 8);
            MqttClient mqttClient = new MqttClient(BROKER_URL, clientId, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);

            mqttClient.setCallback(new MqttCallback() {
                ByteBuffer buffer = ByteBuffer.allocate(65536);

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("⚠️ MQTT 连接断开: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    byte[] payload = message.getPayload();
                    processRawData(payload, buffer);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {}
            });

            System.out.println("正在连接 MQTT Broker: " + BROKER_URL);
            mqttClient.connect(options);
            mqttClient.subscribe(TOPIC);
            System.out.println("✅ MQTT 连接成功，正在实时监听真实数据...\n");

        } catch (MqttException e) {
            System.err.println("❌ MQTT 连接失败: " + e.getMessage());
        }
    }

    /**
     * 模拟拆包逻辑，直接输出控制台
     */
    private static void processRawData(byte[] newData, ByteBuffer buffer) {
        if (buffer.position() + newData.length > buffer.capacity()) {
            buffer.clear(); // 简单防溢出
        }
        buffer.put(newData);
        buffer.flip();

        while (buffer.hasRemaining()) {
            int pos = buffer.position();
            byte firstByte = buffer.get(pos);

            // 【识别 NMEA 数据】
            if (firstByte == '$') {
                int endIdx = -1;
                for (int i = pos; i < buffer.limit(); i++) {
                    if (buffer.get(i) == '\n') {
                        endIdx = i;
                        break;
                    }
                }
                if (endIdx < 0) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }
                int len = endIdx - pos + 1;
                byte[] nmeaBytes = new byte[len];
                buffer.get(nmeaBytes);
                String nmea = new String(nmeaBytes, StandardCharsets.UTF_8).trim();

                // 打印真实 NMEA 数据
                System.out.println("🟢 [NMEA] -> " + nmea);

            }
            // 【识别 RTCM3 数据】
            else if (firstByte == (byte) 0xD3) {
                if (buffer.remaining() < 3) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }
                int frameLen = ((buffer.get(pos + 1) & 0x03) << 8) | (buffer.get(pos + 2) & 0xFF);
                int totalLen = 3 + frameLen + 3;

                if (buffer.remaining() < totalLen) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }

                // CRC 校验防止误判
                if (!validateRtcmCrc(buffer, pos, frameLen)) {
                    buffer.position(pos + 1);
                    continue;
                }

                byte[] rtcmData = new byte[totalLen];
                buffer.get(rtcmData);

                // 调用 DLL 解算真实的 RTCM 并打印双频数据
                parseAndPrintRtcm(rtcmData);

            } else {
                buffer.get(); // 未知字节丢弃
            }
        }
        buffer.clear();
    }

    /**
     * 调用 DLL 解算并打印底层双频科学数据 (P1/L1/P2/L2)
     */
    private static void parseAndPrintRtcm(byte[] rtcmData) {
        if (rtklibContext == null) return;

        RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
        RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(MAX_OBS);

        int count = RtklibNative.INSTANCE.rtklib_parse_rtcm_frame_ex(rtklibContext, rtcmData, rtcmData.length, obsRef, MAX_OBS);

        if (count > 0) {
            System.out.println("🔵 [RTCM 解算] -> 成功解析 " + count + " 颗卫星 (双频):");
            for (int i = 0; i < count; i++) {
                RtklibNative.JavaObs obs = obsArray[i];
                obs.read();

                // 提取卫星ID
                int idLen = 0;
                while (idLen < obs.id.length && obs.id[idLen] != 0) idLen++;
                String satId = new String(obs.id, 0, idLen, StandardCharsets.UTF_8).trim();

                // 打印双频点解算数据：P1, L1, SNR1 以及 P2, L2, SNR2
                // obs.P[0] 是 P1, obs.P[1] 是 P2
                System.out.printf("    🛰️ %-4s | P1: %-12.3f L1: %-12.3f SNR: %-4.1f | P2: %-12.3f L2: %-12.3f SNR: %-4.1f\n",
                        satId,
                        obs.P[0], obs.L[0], obs.snr[0] * 4.0f,
                        obs.P[1], obs.L[1], obs.snr[1] * 4.0f);
            }
            System.out.println("------------------------------------------------------------------------------------------------------------------");
        }
    }

    private static boolean validateRtcmCrc(ByteBuffer buffer, int pos, int frameLen) {
        int crc = 0;
        int dataLen = 3 + frameLen;
        for (int i = 0; i < dataLen; i++) {
            crc = ((crc << 8) & 0xFFFFFF) ^ CRC24Q_TABLE[(crc >>> 16) ^ (buffer.get(pos + i) & 0xFF)];
        }
        int expectedCrc = ((buffer.get(pos + dataLen) & 0xFF) << 16) |
                ((buffer.get(pos + dataLen + 1) & 0xFF) << 8) |
                (buffer.get(pos + dataLen + 2) & 0xFF);
        return crc == expectedCrc;
    }
}