package com.lkd.handler;

import com.alibaba.fastjson.JSON;
import com.lkd.business.MsgHandler;
import com.lkd.common.VMSystem;
import com.lkd.config.TopicConfig;
import com.lkd.contract.VmStatusContract;
import com.lkd.emq.Topic;
import com.lkd.feign.VMService;
import com.lkd.http.vo.TaskViewModel;
import com.lkd.service.impl.TaskServiceImpl;
import com.lkd.vo.VmVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Topic(TopicConfig.VMS_STATUS_TOPIC)
@Component
@Slf4j
public class VMStatusHandler implements MsgHandler {

    @Autowired
    private TaskServiceImpl taskService;

    @Autowired
    private VMService vmService;

    @Override
    public void process(String jsonMsg) throws IOException {
        VmStatusContract vmStatusContract = JSON.parseObject(jsonMsg, VmStatusContract.class);
        if(vmStatusContract == null || vmStatusContract.getStatusInfo() == null || vmStatusContract.getStatusInfo().size() == 0){
            return;
        }
        //如果为非正常状态，则创建维修工单
        if(vmStatusContract.getStatusInfo().stream().anyMatch(s->s.isStatus() == false)){
            VmVO vmVO = vmService.getVMInfo(vmStatusContract.getInnerCode());
            if(vmVO == null){
                return;
            }
            //查询最少工单量用户
            Integer userId = taskService.getLeastUser(vmVO.getRegionId(), true);
            if(userId != 0){
                TaskViewModel task = new TaskViewModel();
                task.setUserId(userId);
                task.setInnerCode(vmVO.getInnerCode());
                task.setProductType(VMSystem.TASK_TYPE_REPAIR);
                task.setCreateType(0);
                task.setDesc(jsonMsg);
                task.setAssignorId(0);
                taskService.createTask(task);
            }
        }
    }
}
