package com.ruoyi.gnss.service;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.Pointer;

/**
 * JNA 接口：用于调用 rtklib_bridge.dll
 *
 * 改造说明：
 * 1. 新增 Context 相关接口
 * 2. 新增带 Context 的解析函数
 * 3. 保留旧接口以兼容现有代码
 * 4. 函数名与DLL导出名完全一致
 *
 */
public interface RtklibNative extends Library {

    // 加载项目根目录下的 rtklib_bridge.dll
    RtklibNative INSTANCE = Native.load("rtklib_bridge", RtklibNative.class);

    /**
     * 观测数据结构体
     */
    @Structure.FieldOrder({"sat", "P", "L", "snr", "id", "code"})
    public static class JavaObs extends Structure {
        public int sat;                      // 卫星号
        public double[] P = new double[3];   // 伪距 (3个频点)
        public double[] L = new double[3];   // 载波相位 (3个频点)
        public float[] snr = new float[3];   // 信噪比 (3个频点)
        public byte[] id = new byte[8];      // 卫星ID字符串
        public byte[] code = new byte[24];   // 信号代码 (3个频点，每个8字节)

        public static class ByReference extends JavaObs implements Structure.ByReference {}
        public static class ByValue extends JavaObs implements Structure.ByValue {}
    }

    /**
     * Context 统计信息结构体
     */
    @Structure.FieldOrder({"totalFrames", "totalObs", "errorCount", "lastMessageType"})
    public static class ContextStats extends Structure {
        public int totalFrames;        // 总帧数
        public int totalObs;           // 总观测值数
        public int errorCount;         // 错误计数
        public int lastMessageType;    // 最后消息类型

        public static class ByReference extends ContextStats implements Structure.ByReference {}
    }

    // ==================== Context 生命周期管理 ====================

    /**
     * 创建新的 Context
     *
     * @param stationId 站点ID（可选，传null使用默认值）
     * @return Context 指针
     */
    Pointer rtklib_create_context(String stationId);

    /**
     * 销毁 Context
     *
     * @param handle Context 指针
     */
    void rtklib_destroy_context(Pointer handle);

    /**
     * 增加 Context 引用计数
     *
     * @param handle Context 指针
     */
    void rtklib_context_addref(Pointer handle);

    /**
     * 获取 Context 信息
     *
     * @param handle Context 指针
     * @param stationIdOut 输出站点ID缓冲区
     * @param stationIdSize 缓冲区大小
     * @return Context ID，失败返回 -1
     */
    int rtklib_get_context_info(Pointer handle, byte[] stationIdOut, int stationIdSize);

    // ==================== RTCM 解析（带 Context） ====================

    /**
     * 解析 RTCM 数据帧（带 Context）
     *
     * @param handle Context 指针
     * @param buff 输入的二进制 RTCM 数据
     * @param len 数据长度
     * @param outObs 输出的观测值数组
     * @param maxObs 数组最大容量
     * @return 解析出的卫星数量，失败返回 -1
     */
    int rtklib_parse_rtcm_frame_ex(Pointer handle, byte[] buff, int len, JavaObs.ByReference outObs, int maxObs);

    // ==================== 工具函数 ====================

    /**
     * 获取 DLL 版本信息
     *
     * @return 版本字符串
     */
    String rtklib_get_version();

    /**
     * 获取当前活跃的 Context 数量
     *
     * @return 活跃 Context 数量
     */
    int rtklib_get_active_context_count();

    /**
     * 重置 Context 的 RTCM 状态
     *
     * @param handle Context 指针
     * @return 0 成功，-1 失败
     */
    int rtklib_reset_context(Pointer handle);

    /**
     * 获取 Context 统计信息
     *
     * @param handle Context 指针
     * @param stats 输出的统计信息结构体
     * @return 0 成功，-1 失败
     */
    int rtklib_get_context_stats(Pointer handle, ContextStats.ByReference stats);

    // ==================== 辅助方法 ====================

    /**
     * 检查 DLL 是否支持多实例模式
     *
     * @return true 表示支持多实例
     */
    static boolean isMultiInstanceSupported() {
        try {
            String version = INSTANCE.rtklib_get_version();
            return version != null && version.contains("Multi-Context");
        } catch (Exception e) {
            return false;
        }
    }
}
