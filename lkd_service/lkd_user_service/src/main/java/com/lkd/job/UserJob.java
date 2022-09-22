package com.lkd.job;

import com.lkd.common.VMSystem;
import com.lkd.service.UserService;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.log.XxlJobLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;

@Component
@Slf4j
public class UserJob {
    @Autowired
    private UserService userService;

    @Autowired
    private RedisTemplate redisTemplate;

    @XxlJob("workCountInitJobHandler")
    //由xxl-job负责处理，每日凌晨生成当天的工单数列表。
    public ReturnT<String> workCountInitJobHandler(String param) {
        XxlJobLogger.log("每日工单量初始化");
        userService.list().forEach(userEntity -> {
            //将管理员排除在外
            if (!userEntity.getRoleCode().equals("1001")) {
                // key的规则，以固定字符串（前缀）+日期+区域+工单类别（运营/运维）为key，
                // 以人员id做为值，以工单数作为分数
                // 过期时间为1天
                String key = VMSystem.REGION_TASK_KEY_PREF
                        + LocalDate.now() + "."
                        + userEntity.getRegionId() + "."
                        + userEntity.getRoleCode();
                redisTemplate.opsForZSet().add(key,userEntity.getId(),0);
                redisTemplate.expire(key, Duration.ofDays(1));
            }
        });
        return ReturnT.SUCCESS;
    }
}
