package com.ruoyi.ieims.gnss.service;

import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
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
 * @date 2026-05-06
 */
@Component
public class RtklibContextManager {
    private static final Logger logger = LoggerFactory.getLogger(RtklibContextManager.class);
    private static final String DEFAULT_STATION = "default";

    // 将 Pointer 和 引用计数合并，彻底解决双 Map 原子性问题
    private final ConcurrentHashMap<String, ContextWrapper> contextMap = new ConcurrentHashMap<>();
    private String dllVersion = "unknown";
    private boolean multiInstanceSupported = false;

    @PostConstruct
    public void init() {
        try {
            dllVersion = RtklibNative.INSTANCE.rtklib_get_version();
            multiInstanceSupported = RtklibNative.isMultiInstanceSupported();
            logger.info("RtklibContextManager 初始化完成...");
        } catch (Exception e) {
            logger.error("初始化失败", e);
        }
    }

    private String normalizeStationId(String stationId) {
        return (stationId == null || stationId.trim().isEmpty()) ? DEFAULT_STATION : stationId;
    }

    public Pointer getOrCreateContext(String stationId) {
        String finalStationId = normalizeStationId(stationId);
        if (!multiInstanceSupported) return null;

        // 使用 compute 分段锁（仅锁住单个哈希桶），取代 synchronized(this) 避免全局阻塞
        ContextWrapper wrapper = contextMap.compute(finalStationId, (key, existing) -> {
            if (existing != null) {
                existing.refCount.incrementAndGet();
                return existing;
            }
            try {
                Pointer newCtx = RtklibNative.INSTANCE.rtklib_create_context(key);
                if (newCtx == null) throw new RuntimeException("底层返回 null");
                return new ContextWrapper(newCtx);
            } catch (Exception e) {
                logger.error("创建 Context 异常: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        });
        return wrapper.pointer;
    }

    public Pointer getContext(String stationId) {
        ContextWrapper wrapper = contextMap.get(normalizeStationId(stationId));
        return wrapper != null ? wrapper.pointer : null;
    }

    public void forceDestroyContext(String stationId) {
        ContextWrapper removed = contextMap.remove(normalizeStationId(stationId));
        if (removed != null) {
            destroyNativeSafe(stationId, removed.pointer);
        }
    }

    // ... 保留 getDllVersion() 等基础方法 ...

    @PreDestroy
    public void destroyAll() {
        for (Map.Entry<String, ContextWrapper> entry : contextMap.entrySet()) {
            destroyNativeSafe(entry.getKey(), entry.getValue().pointer);
        }
        contextMap.clear();
    }

    private void destroyNativeSafe(String stationId, Pointer ptr) {
        try { RtklibNative.INSTANCE.rtklib_destroy_context(ptr); }
        catch (Exception ignored) {}
    }

    private static class ContextWrapper {
        final Pointer pointer;
        final AtomicInteger refCount = new AtomicInteger(1);
        ContextWrapper(Pointer pointer) { this.pointer = pointer; }
    }
}