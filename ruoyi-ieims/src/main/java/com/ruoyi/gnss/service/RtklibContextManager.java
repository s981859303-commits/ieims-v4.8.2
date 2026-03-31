package com.ruoyi.gnss.service;

import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rtklib Context 管理器
 *
 * 功能说明：
 * 1. 管理每个站点的独立 Context/Handle
 * 2. 支持按站点创建、获取、释放 Context
 * 3. 自动清理资源，防止内存泄漏
 * 4. 线程安全
 *
 * 使用方式：
 * <pre>
 * // 获取或创建站点 Context
 * Pointer ctx = contextManager.getOrCreateContext("station_001");
 *
 * // 使用 Context 解析数据
 * int count = RtklibNative.INSTANCE.rtklib_parse_rtcm_frame_ex(ctx, data, len, obs, maxObs);
 *
 * // 释放站点 Context（可选，系统关闭时会自动释放）
 * contextManager.releaseContext("station_001");
 * </pre>
 *
 * @author GNSS Team
 * @date 2026-03-27
 */
@Component
public class RtklibContextManager {

    private static final Logger logger = LoggerFactory.getLogger(RtklibContextManager.class);

    /** 站点ID -> Context 指针映射 */
    private final ConcurrentHashMap<String, Pointer> contextMap = new ConcurrentHashMap<>();

    /** Context 引用计数 */
    private final ConcurrentHashMap<String, AtomicInteger> refCountMap = new ConcurrentHashMap<>();

    /** 是否已初始化 */
    private volatile boolean initialized = false;

    /** DLL 版本 */
    private String dllVersion = "unknown";

    /** 是否支持多实例 */
    private volatile boolean multiInstanceSupported = false;

    /**
     * 初始化（延迟初始化，首次使用时调用）
     */
    private synchronized void ensureInitialized() {
        if (initialized) {
            return;
        }

        try {
            // 获取 DLL 版本
            dllVersion = RtklibNative.INSTANCE.rtklib_get_version();
            multiInstanceSupported = RtklibNative.isMultiInstanceSupported();

            logger.info("RtklibContextManager 初始化完成，DLL 版本: {}, 多实例支持: {}",
                    dllVersion, multiInstanceSupported);
            initialized = true;
        } catch (UnsatisfiedLinkError e) {
            logger.error("找不到 rtklib_bridge.dll，请检查系统路径！");
            throw new RuntimeException("rtklib_bridge.dll 加载失败", e);
        } catch (Exception e) {
            logger.error("RtklibContextManager 初始化失败: {}", e.getMessage());
            throw new RuntimeException("RtklibContextManager 初始化失败", e);
        }
    }

    /**
     * 获取或创建站点 Context
     *
     * @param stationId 站点ID
     * @return Context 指针，如果不支持多实例则返回 null
     */
    public Pointer getOrCreateContext(String stationId) {
        ensureInitialized();

        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }

        final String finalStationId = stationId;

        // 如果不支持多实例，返回 null（调用方应使用兼容模式）
        if (!multiInstanceSupported) {
            logger.debug("DLL 不支持多实例，站点 {} 将使用兼容模式", finalStationId);
            return null;
        }

        return contextMap.computeIfAbsent(stationId, id -> {
            try {
                Pointer ctx = RtklibNative.INSTANCE.rtklib_create_context(id);
                if (ctx == null) {
                    throw new RuntimeException("创建 Context 失败: " + id);
                }
                refCountMap.put(id, new AtomicInteger(1));
                logger.info("创建站点 Context: stationId={}, ptr={}", id, ctx);
                return ctx;
            } catch (Exception e) {
                logger.error("创建 Context 异常: stationId={}, error={}", id, e.getMessage());
                throw new RuntimeException("创建 Context 失败", e);
            }
        });
    }

    /**
     * 获取已存在的 Context（不创建新的）
     *
     * @param stationId 站点ID
     * @return Context 指针，不存在返回 null
     */
    public Pointer getContext(String stationId) {
        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }
        return contextMap.get(stationId);
    }

    /**
     * 增加站点 Context 引用计数
     *
     * @param stationId 站点ID
     */
    public void addRef(String stationId) {
        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }

        AtomicInteger count = refCountMap.get(stationId);
        if (count != null) {
            int newCount = count.incrementAndGet();
            logger.debug("增加引用计数: stationId={}, refCount={}", stationId, newCount);
        }
    }

    /**
     * 释放站点 Context（减少引用计数，为0时销毁）
     *
     * @param stationId 站点ID
     */
    public void releaseContext(String stationId) {
        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }

        AtomicInteger count = refCountMap.get(stationId);
        if (count == null) {
            return;
        }

        int newCount = count.decrementAndGet();
        if (newCount <= 0) {
            // 引用计数为0，销毁 Context
            Pointer ctx = contextMap.remove(stationId);
            refCountMap.remove(stationId);

            if (ctx != null) {
                try {
                    RtklibNative.INSTANCE.rtklib_destroy_context(ctx);
                    logger.info("销毁站点 Context: stationId={}", stationId);
                } catch (Exception e) {
                    logger.error("销毁 Context 异常: stationId={}, error={}", stationId, e.getMessage());
                }
            }
        } else {
            logger.debug("减少引用计数: stationId={}, refCount={}", stationId, newCount);
        }
    }

    /**
     * 强制销毁站点 Context（忽略引用计数）
     *
     * @param stationId 站点ID
     */
    public void forceDestroyContext(String stationId) {
        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }

        Pointer ctx = contextMap.remove(stationId);
        refCountMap.remove(stationId);

        if (ctx != null) {
            try {
                RtklibNative.INSTANCE.rtklib_destroy_context(ctx);
                logger.info("强制销毁站点 Context: stationId={}", stationId);
            } catch (Exception e) {
                logger.error("强制销毁 Context 异常: stationId={}, error={}", stationId, e.getMessage());
            }
        }
    }

    /**
     * 重置站点 Context 的 RTCM 状态
     *
     * @param stationId 站点ID
     * @return true 成功，false 失败
     */
    public boolean resetContext(String stationId) {
        Pointer ctx = getContext(stationId);
        if (ctx == null) {
            logger.warn("重置 Context 失败：站点不存在, stationId={}", stationId);
            return false;
        }

        try {
            int result = RtklibNative.INSTANCE.rtklib_reset_context(ctx);
            if (result == 0) {
                logger.info("重置站点 Context 成功: stationId={}", stationId);
                return true;
            } else {
                logger.error("重置站点 Context 失败: stationId={}, result={}", stationId, result);
                return false;
            }
        } catch (Exception e) {
            logger.error("重置 Context 异常: stationId={}, error={}", stationId, e.getMessage());
            return false;
        }
    }

    /**
     * 获取站点 Context 信息
     *
     * @param stationId 站点ID
     * @return Context 信息，不存在返回 null
     */
    public ContextInfo getContextInfo(String stationId) {
        Pointer ctx = getContext(stationId);
        if (ctx == null) {
            return null;
        }

        try {
            byte[] stationIdBytes = new byte[64];
            int contextId = RtklibNative.INSTANCE.rtklib_get_context_info(ctx, stationIdBytes, 64);
            String contextStationId = new String(stationIdBytes).trim();

            AtomicInteger refCount = refCountMap.get(stationId);
            int ref = refCount != null ? refCount.get() : 0;

            return new ContextInfo(contextId, contextStationId, ref);
        } catch (Exception e) {
            logger.error("获取 Context 信息异常: stationId={}, error={}", stationId, e.getMessage());
            return null;
        }
    }

    /**
     * 获取当前活跃的 Context 数量
     *
     * @return 活跃 Context 数量
     */
    public int getActiveContextCount() {
        try {
            return RtklibNative.INSTANCE.rtklib_get_active_context_count();
        } catch (Exception e) {
            return contextMap.size();
        }
    }

    /**
     * 获取所有站点ID
     *
     * @return 站点ID集合
     */
    public java.util.Set<String> getAllStationIds() {
        return contextMap.keySet();
    }

    /**
     * 获取 DLL 版本
     *
     * @return DLL 版本字符串
     */
    public String getDllVersion() {
        ensureInitialized();
        return dllVersion;
    }

    /**
     * 是否支持多实例
     *
     * @return true 支持多实例
     */
    public boolean isMultiInstanceSupported() {
        ensureInitialized();
        return multiInstanceSupported;
    }

    /**
     * 销毁所有 Context
     */
    @PreDestroy
    public void destroyAll() {
        logger.info("正在销毁所有 Context，共 {} 个...", contextMap.size());

        for (Map.Entry<String, Pointer> entry : contextMap.entrySet()) {
            try {
                RtklibNative.INSTANCE.rtklib_destroy_context(entry.getValue());
                logger.info("销毁站点 Context: stationId={}", entry.getKey());
            } catch (Exception e) {
                logger.error("销毁 Context 异常: stationId={}, error={}", entry.getKey(), e.getMessage());
            }
        }

        contextMap.clear();
        refCountMap.clear();

        logger.info("所有 Context 已销毁");
    }

    /**
     * Context 信息类
     */
    public static class ContextInfo {
        private final int contextId;
        private final String stationId;
        private final int refCount;

        public ContextInfo(int contextId, String stationId, int refCount) {
            this.contextId = contextId;
            this.stationId = stationId;
            this.refCount = refCount;
        }

        public int getContextId() {
            return contextId;
        }

        public String getStationId() {
            return stationId;
        }

        public int getRefCount() {
            return refCount;
        }

        @Override
        public String toString() {
            return String.format("ContextInfo{contextId=%d, stationId='%s', refCount=%d}",
                    contextId, stationId, refCount);
        }
    }
}
