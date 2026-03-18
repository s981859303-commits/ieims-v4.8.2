package com.ruoyi.web.controller.test;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import javax.annotation.security.PermitAll;

@Controller
public class IeimsIndexController {
    /**
     * 访问 /ieims 跳转页面  http://localhost/ieims
     * http://localhost/ieims2
     * return "forward:/xxx" 是服务端内部请求转发，地址栏不变，可转发到接口 / 静态资源路径，适合「路径解耦」「转发前执行业务逻辑」的场景；
     * return "/xxx" 是SpringMVC 直接解析视图，地址栏与请求路径一致，适合「访问路径和页面文件名一致」的简洁场景；
     * 你的核心需求（访问/ieims打开ieims-index.html），用 **@GetMapping("/ieims") + return "forward:/ieims-index.html"** 是最优解，无需为静态页面写多余接口；
     * 关键区别：forward:是服务端内部行为，浏览器无感知，地址栏不变；直接 return 路径是直接解析视图，无转发过程。
     // ===================== 新增：放行 IEIMS 相关路径（核心修改） =====================
     filterChainDefinitionMap.put("/ieims", "anon"); // 放行访问路径 /ieims
     filterChainDefinitionMap.put("/ieims-index.html", "anon"); // 放行对应的静态页面
     //        filterChainDefinitionMap.put("/images/**", "anon"); // 放行图片目录（确保图片正常加载，若已有可忽略）
     // ==============================================================================
     */
    @GetMapping("/ieims")
    public String ieimsIndex() {
        // 转发到静态HTML, 不经解析器
        return "forward:/ieims-index.html";
    }
    @GetMapping("/ieims2")
    public String ieimsIndex2() {
        // 转发到静态HTML, 不经解析器
        return "forward:/ieims-index2.html";
    }
    @GetMapping("/ieims3")
    public String ieimsIndex3() {
        // 经解析器
        return "/ieims2";
    }
}
