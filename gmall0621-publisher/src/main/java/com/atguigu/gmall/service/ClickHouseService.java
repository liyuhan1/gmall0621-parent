package com.atguigu.gmall.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ClickHouseService {

    public BigDecimal getOrderAmount(String date);

    public Map<String, BigDecimal> getOrderAmountHour(String date);

}
