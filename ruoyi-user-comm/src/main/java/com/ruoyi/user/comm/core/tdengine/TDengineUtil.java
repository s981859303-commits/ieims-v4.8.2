package com.ruoyi.user.comm.core.tdengine;

import com.ruoyi.TestTaskService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TDengine 通用操作工具类（纯 JDBC + Druid 版本）
 *
 * <p>
 * 封装 TDengine 时序数据库的通用 CRUD 操作，基于 Spring JdbcTemplate 实现，适配 Druid 连接池。
 * 核心优势：统一异常处理、简化操作API、适配 TDengine 语法特性（超级表、标签、时序查询等）。
 * 适用场景：多模块项目中统一管理 TDengine 操作，避免重复编写数据库交互代码。
 * </p>
 *
 * <p>
 * v2.1 更新内容：
 * 1. 新增 ensureDatabaseExists() 方法，确保数据库存在后再执行 USE 操作
 * 2. 新增 createDatabaseIfNotExists() 方法，自动创建数据库
 * 3. 新增缓存已创建的数据库，避免重复检查
 * </p>
 *
 * @author 工具类维护人
 * @date 2026-03-09
 * @version 2.1 - 2026-04-03 新增数据库自动创建功能
 */
@Component
@Slf4j
public class TDengineUtil {

    /**
     * 注入 Druid 连接池配置的 JdbcTemplate（专用于 TDengine 数据库操作）
     * 必须指定名称，避免注入默认的 MySQL JdbcTemplate
     */
    @Resource(name = "tdengineJdbcTemplate")
    private JdbcTemplate tdengineJdbcTemplate;

    /**
     * 缓存已创建的数据库，避免重复检查和创建
     */
    private static final Set<String> createdDatabases = ConcurrentHashMap.newKeySet();

    // ==================== 数据库管理方法 ====================

    /**
     * 确保数据库存在（如果不存在则创建）
     *
     * <p>
     * 此方法会先检查数据库是否存在，如果不存在则自动创建。
     * 建议在执行任何需要指定数据库的操作之前调用此方法。
     * </p>
     *
     * @param database 数据库名称
     * @throws RuntimeException 创建数据库失败时抛出异常
     */
    public void ensureDatabaseExists(String database) {
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("数据库名称不能为空");
        }

        // 检查缓存
        if (createdDatabases.contains(database)) {
            log.debug("数据库已存在于缓存中: {}", database);
            return;
        }

        try {
            // 检查数据库是否存在
            String checkSql = "SHOW DATABASES WHERE name = '" + database + "'";
            List<Map<String, Object>> result = tdengineJdbcTemplate.queryForList(checkSql);

            if (result.isEmpty()) {
                // 数据库不存在，创建它
                createDatabaseIfNotExists(database);
            }

            // 添加到缓存
            createdDatabases.add(database);
            log.info("数据库已确认存在: {}", database);

        } catch (Exception e) {
            log.warn("检查数据库存在性失败，尝试直接创建: {}", e.getMessage());
            // 尝试直接创建
            try {
                createDatabaseIfNotExists(database);
                createdDatabases.add(database);
            } catch (Exception ex) {
                throw new RuntimeException("确保数据库存在失败: " + database, ex);
            }
        }
    }

    /**
     * 创建数据库（如果不存在）
     *
     * <p>
     * 使用默认参数创建数据库：
     * - KEEP 365：保留365天数据
     * - DAYS 10：每个数据文件存储10天数据
     * - BLOCKS 6：内存块数量
     * </p>
     *
     * @param database 数据库名称
     * @throws RuntimeException 创建数据库失败时抛出异常
     */
    public void createDatabaseIfNotExists(String database) {
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("数据库名称不能为空");
        }

        String sql = String.format(
                "CREATE DATABASE IF NOT EXISTS %s KEEP 365 DAYS 10 BLOCKS 6",
                database
        );

        try {
            tdengineJdbcTemplate.execute(sql);
            log.info("数据库已创建/确认: {}", database);
        } catch (Exception e) {
            throw new RuntimeException("创建数据库失败: " + database, e);
        }
    }

    /**
     * 切换到指定数据库（确保数据库存在）
     *
     * <p>
     * 此方法会先确保数据库存在，然后切换到该数据库。
     * 推荐使用此方法替代直接执行 "USE database"。
     * </p>
     *
     * @param database 数据库名称
     * @throws RuntimeException 切换数据库失败时抛出异常
     */
    public void useDatabase(String database) {
        ensureDatabaseExists(database);
        executeDDL("USE " + database);
        log.debug("已切换到数据库: {}", database);
    }

    // ==================== DDL/DML 操作方法 ====================

    /**
     * 执行 DDL（数据定义语言）语句
     *
     * <p>
     * 适用于创建/删除数据库、超级表、子表，修改表结构等操作，无返回值。
     * TDengine 常用 DDL 场景：CREATE DATABASE、CREATE STABLE、DROP TABLE、ALTER TABLE 等。
     * </p>
     *
     * @param sql 待执行的 DDL 语句（如：CREATE STABLE IF NOT EXISTS test.st_data (ts TIMESTAMP, value FLOAT) TAGS (device_id INT)）
     * @throws RuntimeException 执行失败时抛出运行时异常，包含具体 SQL 和错误信息
     */
    public void executeDDL(String sql) {
        try {
            tdengineJdbcTemplate.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException("TDengine执行DDL失败：" + sql, e);
        }
    }

    /**
     * 执行单条 DML（数据操纵语言）语句（插入/更新/删除）
     *
     * <p>
     * 适用于单条数据的增删改操作，返回受影响的行数。
     * TDengine 常用 DML 场景：INSERT 单条时序数据、DELETE 指定时间范围数据、UPDATE 标签值等。
     * </p>
     *
     * @param sql    待执行的 DML 语句（支持参数占位符 ?，如：INSERT INTO test.tb_001 (ts, value) VALUES (?, ?)）
     * @param params SQL 占位符对应的参数列表（顺序与占位符一致）
     * @return 受影响的行数（TDengine 中 INSERT 单条数据通常返回 1）
     * @throws RuntimeException 执行失败时抛出运行时异常，包含具体 SQL 和错误信息
     */
    public int executeUpdate(String sql, Object... params) {
        try {
            return tdengineJdbcTemplate.update(sql, params);
        } catch (Exception e) {
            throw new RuntimeException("TDengine执行DML失败：" + sql, e);
        }
    }

    /**
     * 批量执行 DML 语句（高性能批量插入/更新）
     *
     * <p>
     * 适用于大批量时序数据写入（TDengine 核心场景），相比单条插入性能提升 10 倍以上。
     * 底层基于 JdbcTemplate 批量操作，减少数据库连接交互次数，适配 TDengine 批量写入优化。
     * </p>
     *
     * @param sql           待执行的批量 DML 语句（如：INSERT INTO test.tb_001 (ts, value) VALUES (?, ?)）
     * @param batchParams   批量参数列表，每个 Object[] 对应一条数据的参数（与 SQL 占位符顺序一致）
     * @return 每个批次受影响的行数数组（数组长度与 batchParams 一致，每个元素为对应批次的影响行数）
     * @throws RuntimeException 执行失败时抛出运行时异常，包含具体 SQL 和错误信息
     */
    public int[] batchUpdate(String sql, List<Object[]> batchParams) {
        try {
            return tdengineJdbcTemplate.batchUpdate(sql, batchParams);
        } catch (Exception e) {
            throw new RuntimeException("TDengine批量操作失败：" + sql, e);
        }
    }

    /**
     * 查询单行数据，返回 Map 格式（键为列名，值为列值）
     *
     * <p>
     * 适用于查询单条时序数据、统计结果（如：查询某设备某时间点的数值），若结果为空或多行则抛出异常。
     * TDengine 常用场景：SELECT * FROM test.tb_001 WHERE ts = ? LIMIT 1、SELECT COUNT(*) FROM test.st_data WHERE device_id = ?
     * </p>
     *
     * @param sql    查询 SQL 语句（支持参数占位符 ?）
     * @param params SQL 占位符对应的参数列表
     * @return 单行数据的 Map 结果（列名 -> 列值，列名与 TDengine 表字段一致，区分大小写）
     * @throws RuntimeException 执行失败/结果为空/结果多行时抛出运行时异常
     */
    public Map<String, Object> queryForMap(String sql, Object... params) {
        try {
            return tdengineJdbcTemplate.queryForMap(sql, params);
        } catch (Exception e) {
            throw new RuntimeException("TDengine查询单行失败：" + sql, e);
        }
    }

    /**
     * 查询多行数据，返回 List<Map> 格式
     *
     * <p>
     * 适用于查询指定时间范围的时序数据、多设备聚合数据等批量查询场景，结果为空时返回空 List。
     * TDengine 常用场景：SELECT * FROM test.st_data WHERE ts BETWEEN ? AND ? AND device_id = ?
     * </p>
     *
     * @param sql    查询 SQL 语句（支持参数占位符 ?）
     * @param params SQL 占位符对应的参数列表
     * @return 多行数据的 List 结果，每个 Map 对应一行数据（列名 -> 列值）
     * @throws RuntimeException 执行失败时抛出运行时异常，包含具体 SQL 和错误信息
     */
    public List<Map<String, Object>> queryForList(String sql, Object... params) {
        try {
            return tdengineJdbcTemplate.queryForList(sql, params);
        } catch (Exception e) {
            throw new RuntimeException("TDengine查询多行失败：" + sql, e);
        }
    }

    /**
     * 查询数据并映射为指定实体类列表
     *
     * <p>
     * 适用于需要将查询结果转换为业务实体的场景，通过 RowMapper 自定义字段与实体属性的映射关系，简化数据处理。
     * TDengine 常用场景：查询时序数据并封装为 DeviceDataDTO 等业务对象。
     * </p>
     *
     * @param <T>       实体类泛型
     * @param sql       查询 SQL 语句（支持参数占位符 ?）
     * @param rowMapper 行映射器，自定义字段到实体属性的映射规则（如：(rs, rowNum) -> new DeviceDataDTO(rs.getTimestamp("ts"), rs.getFloat("value"))）
     * @param params    SQL 占位符对应的参数列表
     * @return 实体类列表，每个元素为映射后的业务对象
     * @throws RuntimeException 执行失败时抛出运行时异常，包含具体 SQL 和错误信息
     */
    public <T> List<T> queryForEntityList(String sql, RowMapper<T> rowMapper, Object... params) {
        try {
            return tdengineJdbcTemplate.query(sql, rowMapper, params);
        } catch (Exception e) {
            throw new RuntimeException("TDengine查询实体类失败：" + sql, e);
        }
    }

    /**
     * 查询单个值（如 COUNT、SUM、MAX 等聚合函数结果）
     *
     * <p>
     * 适用于统计类查询，返回单个标量值（如：数据条数、平均值、最大值），若结果为空则抛出异常。
     * TDengine 常用场景：SELECT COUNT(*) FROM test.st_data、SELECT MAX(value) FROM test.tb_001 WHERE ts BETWEEN ? AND ?
     * </p>
     *
     * @param <T>    返回值类型泛型（如 Integer、Long、Float、String 等）
     * @param sql    查询 SQL 语句（支持参数占位符 ?）
     * @param clazz  返回值类型的 Class 对象（如：Integer.class、Float.class）
     * @param params SQL 占位符对应的参数列表
     * @return 聚合查询的单个结果值
     * @throws RuntimeException 执行失败/结果为空时抛出运行时异常
     */
    public <T> T queryForObject(String sql, Class<T> clazz, Object... params) {
        try {
            return tdengineJdbcTemplate.queryForObject(sql, clazz, params);
        } catch (Exception e) {
            throw new RuntimeException("TDengine查询单个值失败：" + sql, e);
        }
    }

    /**
     * 【简化版】查询多行数据，返回 List<Map> 格式（无异常封装，直接返回 JdbcTemplate 结果）
     *
     * <p>
     * 适用于不需要统一异常处理的简单查询场景，与 queryForList 功能一致，但无自定义异常封装。
     * </p>
     *
     * @param sql  查询 SQL 语句（支持参数占位符 ?）
     * @param args SQL 占位符对应的参数列表
     * @return 多行数据的 List 结果，每个 Map 对应一行数据
     */
    public List<Map<String, Object>> queryForList2(String sql, Object... args) {
        return tdengineJdbcTemplate.queryForList(sql, args);
    }

    /**
     * 【简化版】执行单条 DML 语句（无异常封装，直接返回 JdbcTemplate 结果）
     *
     * <p>
     * 适用于不需要统一异常处理的简单增删改场景，与 executeUpdate 功能一致，但无自定义异常封装。
     * </p>
     *
     * @param sql  DML 语句（支持参数占位符 ?）
     * @param args SQL 占位符对应的参数列表
     * @return 受影响的行数
     */
    public int update2(String sql, Object... args) {
        return tdengineJdbcTemplate.update(sql, args);
    }

    /**
     * 【简化版】执行 DDL 语句（无异常封装，直接执行）
     *
     * <p>
     * 适用于不需要统一异常处理的简单 DDL 场景，与 executeDDL 功能一致，但无自定义异常封装。
     * </p>
     *
     * @param sql DDL 语句
     */
    public void execute2(String sql) {
        tdengineJdbcTemplate.execute(sql);
    }
}