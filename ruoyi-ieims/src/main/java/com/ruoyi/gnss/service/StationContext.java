package com.ruoyi.gnss.service;

/**
 * 站点上下文管理器
 *
 * <p>
 * 功能说明：
 * 1. 管理当前线程关联的站点ID
 * 2. 支持多站点并发处理
 * 3. 使用 ThreadLocal 实现线程隔离
 * </p>
 *
 * <p>
 * 使用方式：
 * <pre>
 * try (StationContext.Scope scope = StationContext.withStation("station_001")) {
 *     // 在此范围内，所有操作都关联到 station_001
 *     mixedLogSplitter.pushData(data);
 * }
 * </pre>
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-26
 */
public class StationContext {

    /** 当前线程关联的站点ID */
    private static final ThreadLocal<String> CURRENT_STATION = new ThreadLocal<>();

    /** 默认站点ID */
    private static volatile String defaultStationId = "default";

    /**
     * 私有构造函数
     */
    private StationContext() {
    }

    /**
     * 设置默认站点ID
     *
     * @param stationId 默认站点ID
     */
    public static void setDefaultStationId(String stationId) {
        if (stationId != null && !stationId.isEmpty()) {
            defaultStationId = stationId;
        }
    }

    /**
     * 获取默认站点ID
     *
     * @return 默认站点ID
     */
    public static String getDefaultStationId() {
        return defaultStationId;
    }

    /**
     * 获取当前站点ID
     *
     * @return 当前站点ID，如果未设置则返回默认站点ID
     */
    public static String getCurrentStationId() {
        String stationId = CURRENT_STATION.get();
        return stationId != null ? stationId : defaultStationId;
    }

    /**
     * 设置当前站点ID
     *
     * @param stationId 站点ID
     */
    public static void setCurrentStationId(String stationId) {
        CURRENT_STATION.set(stationId);
    }

    /**
     * 清除当前站点ID
     */
    public static void clear() {
        CURRENT_STATION.remove();
    }

    /**
     * 判断是否已设置站点ID
     *
     * @return true 表示已设置
     */
    public static boolean hasStationId() {
        return CURRENT_STATION.get() != null;
    }

    /**
     * 创建一个站点作用域
     *
     * @param stationId 站点ID
     * @return 作用域对象，使用 try-with-resources 自动清理
     */
    public static Scope withStation(String stationId) {
        return new Scope(stationId);
    }

    /**
     * 站点作用域
     *
     * <p>
     * 实现 AutoCloseable 接口，支持 try-with-resources 语法
     * </p>
     */
    public static class Scope implements AutoCloseable {

        private final String previousStationId;

        /**
         * 构造函数
         *
         * @param stationId 站点ID
         */
        private Scope(String stationId) {
            this.previousStationId = CURRENT_STATION.get();
            CURRENT_STATION.set(stationId);
        }

        @Override
        public void close() {
            if (previousStationId != null) {
                CURRENT_STATION.set(previousStationId);
            } else {
                CURRENT_STATION.remove();
            }
        }
    }

    /**
     * 在指定站点上下文中执行任务
     *
     * @param stationId 站点ID
     * @param runnable  要执行的任务
     */
    public static void runWithStation(String stationId, Runnable runnable) {
        try (Scope scope = withStation(stationId)) {
            runnable.run();
        }
    }

    /**
     * 在指定站点上下文中执行任务并返回结果
     *
     * @param stationId 站点ID
     * @param supplier  要执行的任务
     * @param <T>       返回类型
     * @return 任务执行结果
     */
    public static <T> T callWithStation(String stationId, java.util.function.Supplier<T> supplier) {
        try (Scope scope = withStation(stationId)) {
            return supplier.get();
        }
    }
}
