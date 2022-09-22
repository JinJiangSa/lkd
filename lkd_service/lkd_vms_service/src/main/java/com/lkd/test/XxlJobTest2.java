package com.lkd.test;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.stereotype.Component;

@Component
public class XxlJobTest2 {
    @XxlJob("demoJobHandler")
    public ReturnT<String> helloJob(String param){
        System.out.println("简单任务执行了。。。。"+param);
        return ReturnT.SUCCESS;
    }
}
