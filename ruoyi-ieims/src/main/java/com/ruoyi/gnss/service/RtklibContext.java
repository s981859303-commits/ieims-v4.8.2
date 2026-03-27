package com.ruoyi.gnss.service;

import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RTKLIB 解析上下文
 *
 * <p>
 * 封装独立的 RTKLIB 解析器实例，支持多站点并发。
 * 每个站点应该拥有独立的 RtklibContext 实例。
 * </p>
 *
 * <p>
 * 使用方式：
 * <pre>
 * RtklibContext context = new RtklibContext("station_001");
 * try {
 *     int count = context.parseRtcm(rtcmData);
 *     // 处理解析结果...
 * } finally {
 *     context.close(); // 或使用 try-with-resources
 * }
 * </pre>
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-26
 */
public class RtklibContext implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RtklibContext.class);

    /** 默认最大观测值数量 */
    private static final int DEFAULT_MAX_OBS = 64;

    /** 站点ID */
    private final String stationId;

    /** Native 句柄 */
    private volatile Pointer handle;

    /** 是否已关闭 */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** 是否使用兼容模式（旧版DLL） */
    private final boolean compatibilityMode;

    /** 统计信息 */
    private final AtomicLong parseCount = new AtomicLong(0);
    private final AtomicLong totalSatCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    /** 创建时间 */
    private final long createTime;

    /**
     * 构造函数
     *
     * @param stationId 站点ID
     */
    public RtklibContext(String stationId) {
        this.stationId = stationId;
        this.createTime = System.currentTimeMillis();

        // 检查是否支持多实例模式
        boolean multiSupported = RtklibNative.isMultiInstanceSupported();

        if (multiSupported) {
            this.handle = RtklibNative.INSTANCE.rtcm_create_context();
            if (this.handle == null) {
                throw new RuntimeException("Failed to create RTKLIB context for station: " + stationId);
            }
            this.compatibilityMode = false;
            logger.info("创建 RTKLIB 上下文成功，站点: {}, 模式: 多实例", stationId);
        } else {
            // 降级到兼容模式
            this.handle = null;
            this.compatibilityMode = true;
            logger.warn("DLL 不支持多实例模式，站点 {} 使用兼容模式（可能有状态污染风险）", stationId);
        }
    }

    /**
     * 解析 RTCM 数据
     *
     * @param rtcmData RTCM 二进制数据
     * @return 解析出的观测值数组，失败返回空数组
     */
    public RtklibNative.JavaObs[] parseRtcm(byte[] rtcmData) {
        return parseRtcm(rtcmData, DEFAULT_MAX_OBS);
    }

    /**
     * 解析 RTCM 数据
     *
     * @param rtcmData RTCM 二进制数据
     * @param maxObs   最大观测值数量
     * @return 解析出的观测值数组
     */
    public RtklibNative.JavaObs[] parseRtcm(byte[] rtcmData, int maxObs) {
        if (closed.get()) {
            logger.warn("上下文已关闭，无法解析数据，站点: {}", stationId);
            return new RtklibNative.JavaObs[0];
        }

        if (rtcmData == null || rtcmData.length == 0) {
            return new RtklibNative.JavaObs[0];
        }

        try {
            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(maxObs);

            int count;
            if (compatibilityMode) {
                // 兼容模式：使用全局单例
                count = RtklibNative.INSTANCE.parse_rtcm_frame(rtcmData, rtcmData.length, obsRef, maxObs);
            } else {
                // 多实例模式：使用独立上下文
                count = RtklibNative.INSTANCE.rtcm_parse_frame_ex(handle, rtcmData, rtcmData.length, obsRef, maxObs);
            }

            parseCount.incrementAndGet();

            if (count < 0) {
                errorCount.incrementAndGet();
                logger.warn("RTCM 解析错误，站点: {}, 错误码: {}", stationId, count);
                return new RtklibNative.JavaObs[0];
            }

            if (count == 0) {
                return new RtklibNative.JavaObs[0];
            }

            totalSatCount.addAndGet(count);

            // 复制结果
            RtklibNative.JavaObs[] result = new RtklibNative.JavaObs[count];
            System.arraycopy(obsArray, 0, result, 0, count);
            return result;

        } catch (UnsatisfiedLinkError e) {
            errorCount.incrementAndGet();
            logger.error("找不到 rtklib_bridge.dll，站点: {}", stationId);
            throw e;
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("RTCM 解析异常，站点: {}, 错误: {}", stationId, e.getMessage());
            return new RtklibNative.JavaObs[0];
        }
    }

    /**
     * 重置上下文状态
     */
    public void reset() {
        if (closed.get()) {
            return;
        }

        if (!compatibilityMode && handle != null) {
            try {
                RtklibNative.INSTANCE.rtcm_reset_context(handle);
                logger.info("重置 RTKLIB 上下文，站点: {}", stationId);
            } catch (Exception e) {
                logger.error("重置上下文失败，站点: {}, 错误: {}", stationId, e.getMessage());
            }
        }

        parseCount.set(0);
        totalSatCount.set(0);
        errorCount.set(0);
    }

    /**
     * 获取统计信息
     *
     * @return 统计信息字符串
     */
    public String getStatistics() {
        RtklibNative.ContextStats.ByReference stats = null;

        if (!compatibilityMode && handle != null) {
            try {
                stats = new RtklibNative.ContextStats.ByReference();
                RtklibNative.INSTANCE.rtcm_get_context_stats(handle, stats);
            } catch (Exception ignored) {
            }
        }

        return String.format("RTKLIB[station=%s, mode=%s, parses=%d, sats=%d, errors=%d%s]",
                stationId,
                compatibilityMode ? "compat" : "multi",
                parseCount.get(),
                totalSatCount.get(),
                errorCount.get(),
                stats != null ? String.format(", frames=%d", stats.totalFrames) : ""
        );
    }

    /**
     * 获取站点ID
     */
    public String getStationId() {
        return stationId;
    }

    /**
     * 是否已关闭
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * 是否使用兼容模式
     */
    public boolean isCompatibilityMode() {
        return compatibilityMode;
    }

    /**
     * 获取解析次数
     */
    public long getParseCount() {
        return parseCount.get();
    }

    /**
     * 获取总卫星数
     */
    public long getTotalSatCount() {
        return totalSatCount.get();
    }

    /**
     * 获取错误次数
     */
    public long getErrorCount() {
        return errorCount.get();
    }

    /**
     * 获取运行时长（毫秒）
     */
    public long getUptimeMs() {
        return System.currentTimeMillis() - createTime;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (!compatibilityMode && handle != null) {
                try {
                    RtklibNative.INSTANCE.rtcm_destroy_context(handle);
                    logger.info("销毁 RTKLIB 上下文，站点: {}", stationId);
                } catch (Exception e) {
                    logger.error("销毁上下文失败，站点: {}, 错误: {}", stationId, e.getMessage());
                }
                handle = null;
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    @Override
    public String toString() {
        return getStatistics();
    }
}
