package com.ruoyi.ieims.util;

public final class TecMappingFunctionUtil {
    private TecMappingFunctionUtil() {}

    /** 单层模型（SLM）映射函数 */
    public static double calculateSlm(double elevation) {
        if (elevation <= 0.0) return Double.MAX_VALUE;
        double elevRad = Math.toRadians(elevation);
        double cosElev = Math.cos(elevRad);
        double ratio = (TecConstant.EARTH_RADIUS * cosElev)
                / (TecConstant.EARTH_RADIUS + TecConstant.IONOSPHERE_HEIGHT);
        return 1.0 / Math.sqrt(1.0 - ratio * ratio);
    }

    /**
     * 计算电离层穿刺点（IPP）坐标
     * @param stationLat 测站纬度（°）
     * @param stationLon 测站经度（°）
     * @param elevation  卫星仰角（°）
     * @param azimuth    卫星方位角（°）
     * @return [ippLat, ippLon]（°）
     */
    public static double[] calculateIpp(double stationLat, double stationLon,
                                        double elevation, double azimuth) {
        double re = TecConstant.EARTH_RADIUS;
        double h = TecConstant.IONOSPHERE_HEIGHT;
        double latRad = Math.toRadians(stationLat);
        double lonRad = Math.toRadians(stationLon);
        double elevRad = Math.toRadians(elevation);
        double aziRad = Math.toRadians(azimuth);

        // 地心角 ψ
        double psi = Math.PI / 2.0 - elevRad
                - Math.asin(re * Math.cos(elevRad) / (re + h));

        double sinLat = Math.sin(latRad);
        double cosLat = Math.cos(latRad);
        double sinPsi = Math.sin(psi);
        double cosPsi = Math.cos(psi);
        double sinAzi = Math.sin(aziRad);
        double cosAzi = Math.cos(aziRad);

        double ippLatRad = Math.asin(sinLat * cosPsi + cosLat * sinPsi * cosAzi);
        double ippLonRad = lonRad + Math.atan2(sinPsi * sinAzi,
                cosPsi * cosLat - sinLat * sinPsi * cosAzi);

        return new double[]{Math.toDegrees(ippLatRad), Math.toDegrees(ippLonRad)};
    }
}