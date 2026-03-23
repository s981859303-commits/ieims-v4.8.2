package com.ruoyi.gnss;

import com.ruoyi.gnss.service.RtklibNative;

public class TestRtklibDll {

    public static void main(String[] args) {
        System.out.println("🚀 正在加载 RTKLIB DLL...");

        try {
            // 1. 尝试调用一下，看看会不会报错
            // 随便造一点伪数据 (模拟 RTCM 数据包)
            // 注意：因为是乱造的数据，C代码肯定解析不出结果，但只要不报错(Crash)，就说明调用成功了
            byte[] fakeData = new byte[100];
            fakeData[0] = (byte) 0xD3; // RTCM 帧头

            // 准备内存空间 (接收结果)
            RtklibNative.JavaObs.ByReference obsArray = new RtklibNative.JavaObs.ByReference();
            // 申请能存 32 个卫星数据的连续内存
            RtklibNative.JavaObs[] output = (RtklibNative.JavaObs[]) obsArray.toArray(32);

            System.out.println("✅ DLL 加载成功！开始调用 C 函数...");

            // 2. 调用 C 函数
            int count = RtklibNative.INSTANCE.parse_rtcm_frame(fakeData, fakeData.length, obsArray, 32);

            System.out.println("✅ 调用完成！解析到的卫星数: " + count);
            System.out.println("🎉 恭喜！Java 与 C 语言联调成功！");

        } catch (UnsatisfiedLinkError e) {
            System.err.println("❌ 加载失败：找不到 DLL 文件！");
            System.err.println("请确保 'rtklib_bridge.dll' 位于项目根目录，或者系统 PATH 环境变量中。");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}