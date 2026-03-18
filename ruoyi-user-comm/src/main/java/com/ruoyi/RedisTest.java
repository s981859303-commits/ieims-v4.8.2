package com.ruoyi;

import com.ruoyi.user.comm.core.redis.RedisCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * RedisCache 使用示例
 * 演示 RedisCache 中各类方法的调用方式
 */
//@Component
public class RedisTest {
    @Autowired
    private RedisCache redisCache;

    /**
     * 执行所有 Redis 操作示例
     */
    @PostConstruct // 此时 redisCache 已被成功注入
    public void runAllExamples() {
        testStringOperations();
        testListOperations();
        testSetOperations();
        testHashOperations();
        testKeyOperations();
    }

    /**
     * 测试 String 类型操作（基本对象缓存）
     */
    public void testStringOperations() {
        System.out.println("=== 测试 String 类型操作 ===");

        // 1. 缓存普通字符串
        redisCache.setCacheObject("test:str:hello", "Hello Redis!");
        String strValue = redisCache.getCacheObject("test:str:hello");
        System.out.println("获取字符串缓存: " + strValue); // 输出: Hello Redis!

        // 2. 缓存带过期时间的对象（30秒后自动过期）
        User testUser = new User(1L, "张三", 25);
        redisCache.setCacheObject("test:str:user", testUser, 30, TimeUnit.SECONDS);
        User cachedUser = redisCache.getCacheObject("test:str:user");
        System.out.println("获取用户对象缓存: " + cachedUser); // 输出: User(id=1, name=张三, age=25)

        // 3. 覆盖缓存
        redisCache.setCacheObject("test:str:hello", "Hello Updated!");
        System.out.println("覆盖后的值: " + redisCache.getCacheObject("test:str:hello")); // 输出: Hello Updated!
    }

    /**
     * 测试 List 类型操作
     */
    public void testListOperations() {
        System.out.println("\n=== 测试 List 类型操作 ===");

        // 1. 缓存 List
        List<String> fruitList = Arrays.asList("Apple", "Banana", "Cherry");
        long count = redisCache.setCacheList("test:list:fruits", fruitList);
        System.out.println("缓存 List 元素个数: " + count); // 输出: 3

        // 2. 获取 List
        List<String> cachedFruits = redisCache.getCacheList("test:list:fruits");
        System.out.println("获取 List 缓存: " + cachedFruits); // 输出: [Apple, Banana, Cherry]
    }

    /**
     * 测试 Set 类型操作
     */
    public void testSetOperations() {
        System.out.println("\n=== 测试 Set 类型操作 ===");

        // 1. 缓存 Set
        Set<Integer> numberSet = new HashSet<>(Arrays.asList(1, 2, 3, 3, 4)); // Set自动去重
        redisCache.setCacheSet("test:set:numbers", numberSet);

        // 2. 获取 Set
        Set<Integer> cachedNumbers = redisCache.getCacheSet("test:set:numbers");
        System.out.println("获取 Set 缓存（去重后）: " + cachedNumbers); // 输出: [1, 2, 3, 4]
    }

    /**
     * 测试 Hash 类型操作
     */
    public void testHashOperations() {
        System.out.println("\n=== 测试 Hash 类型操作 ===");

        // 1. 缓存整个 Map
        Map<String, String> userMap = new HashMap<>();
        userMap.put("name", "李四");
        userMap.put("age", "30");
        userMap.put("gender", "男");
        redisCache.setCacheMap("test:hash:user", userMap);

        // 2. 获取整个 Map
        Map<String, String> cachedUserMap = redisCache.getCacheMap("test:hash:user");
        System.out.println("获取整个 Hash 缓存: " + cachedUserMap); // 输出: {name=李四, age=30, gender=男}

        // 3. 获取 Hash 中单个字段
        String userName = redisCache.getCacheMapValue("test:hash:user", "name");
        System.out.println("获取 Hash 中 name 字段: " + userName); // 输出: 李四

        // 4. 向 Hash 中添加新字段
        redisCache.setCacheMapValue("test:hash:user", "email", "lisi@example.com");
        System.out.println("添加 email 字段后的 Hash: " + redisCache.getCacheMap("test:hash:user"));

        // 5. 删除 Hash 中字段
        redisCache.deleteCacheMapValue("test:hash:user", "gender");
        System.out.println("删除 gender 字段后的 Hash: " + redisCache.getCacheMap("test:hash:user"));
    }

    /**
     * 测试 Key 相关操作
     */
    public void testKeyOperations() {
        System.out.println("\n=== 测试 Key 相关操作 ===");

        String testKey = "test:key:temp";

        // 1. 判断 Key 是否存在
        boolean existsBefore = redisCache.hasKey(testKey);
        System.out.println("Key 是否存在（写入前）: " + existsBefore); // 输出: false

        // 2. 写入 Key 并设置过期时间（10秒）
        redisCache.setCacheObject(testKey, "临时数据", 10, TimeUnit.SECONDS);
        boolean existsAfter = redisCache.hasKey(testKey);
        System.out.println("Key 是否存在（写入后）: " + existsAfter); // 输出: true

        // 3. 获取 Key 剩余过期时间
        long ttl = redisCache.getExpire(testKey);
        System.out.println("Key 剩余过期时间（秒）: " + ttl); // 输出: 10 左右

        // 4. 延长 Key 过期时间
        redisCache.expire(testKey, 30); // 延长至 30 秒
        System.out.println("延长后剩余过期时间: " + redisCache.getExpire(testKey)); // 输出: 30

        // 5. 删除 Key
        boolean deleted = redisCache.deleteObject(testKey);
        System.out.println("删除 Key 是否成功: " + deleted); // 输出: true
        System.out.println("删除后 Key 是否存在: " + redisCache.hasKey(testKey)); // 输出: false

        // 6. 批量删除 Key
        Collection<String> keysToDelete = Arrays.asList("test:str:hello", "test:str:user");
        boolean batchDeleted = redisCache.deleteObject(keysToDelete);
        System.out.println("批量删除是否成功: " + batchDeleted); // 输出: true

        // 7. 模糊查询 Key
        Collection<String> keys = redisCache.keys("test:*");
        System.out.println("匹配 'test:*' 的 Key: " + keys); // 输出剩余的 test 前缀 Key
    }

    /**
     * 示例实体类
     */
    public static class User {
        private Long id;
        private String name;
        private Integer age;

        public User() {}

        public User(Long id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        // Getter & Setter
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Integer getAge() { return age; }
        public void setAge(Integer age) { this.age = age; }

        @Override
        public String toString() {
            return "User{id=" + id + ", name='" + name + "', age=" + age + "}";
        }
    }
}