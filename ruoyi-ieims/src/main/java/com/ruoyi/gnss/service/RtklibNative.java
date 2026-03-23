package com.ruoyi.gnss.service;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import java.util.Arrays;
import java.util.List;

/**
 * JNA 接口：用于调用 rtklib_bridge.dll
 */
public interface RtklibNative extends Library {

    // 加载项目根目录下的 rtklib_bridge.dll
    RtklibNative INSTANCE = Native.load("rtklib_bridge", RtklibNative.class);

    // 🔥 必须把 id 加到 FieldOrder 里
    @Structure.FieldOrder({"sat", "P", "L", "snr", "id", "code"})
    public static class JavaObs extends Structure {
        public int sat;
        public double[] P = new double[3];
        public double[] L = new double[3];
        public float[] snr = new float[3];
        public byte[] id = new byte[8];

        // 🔥 新增：对应 C 的 char code[3][8]，总共 24 字节
        // JNA 处理二维数组比较麻烦，我们用一个长的一维数组接住它
        public byte[] code = new byte[24];

        public static class ByReference extends JavaObs implements Structure.ByReference {}
        public static class ByValue extends JavaObs implements Structure.ByValue {}
    }

    /**
     * 调用 C 函数解析 RTCM 数据
     * @param buff    输入的二进制 RTCM 数据
     * @param len     数据长度
     * @param outObs  输出的观测值数组（内存由 Java 分配）
     * @param maxObs  数组最大容量（防止 C 代码写越界）
     * @return        解析出的卫星数量
     */
    int parse_rtcm_frame(byte[] buff, int len, JavaObs.ByReference outObs, int maxObs);
}