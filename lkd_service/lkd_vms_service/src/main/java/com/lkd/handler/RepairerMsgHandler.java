package com.lkd.handler;

import com.alibaba.fastjson.JSON;
import com.lkd.business.MsgHandler;
import com.lkd.common.VMSystem;
import com.lkd.contract.TaskCompleteContract;
import com.lkd.emq.Topic;
import com.lkd.service.VendingMachineService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Topic("server/vms/completed")
@Service
@Slf4j
public class RepairerMsgHandler implements MsgHandler{

    @Autowired
    private VendingMachineService vendingMachineService;

    @Override
    public void process(String jsonMsg) throws IOException {
        log.info("处理消息：{}",jsonMsg);
        TaskCompleteContract taskCompleteContract = JSON.parseObject(jsonMsg, TaskCompleteContract.class);
        vendingMachineService.updateStatus(taskCompleteContract.getInnerCode(), VMSystem.VM_STATUS_RUNNING);

    }
}
