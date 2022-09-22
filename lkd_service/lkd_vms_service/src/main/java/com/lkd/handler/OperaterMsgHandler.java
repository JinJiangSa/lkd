package com.lkd.handler;

import com.alibaba.fastjson.JSON;
import com.lkd.business.MsgHandler;
import com.lkd.common.VMSystem;
import com.lkd.contract.SupplyChannel;
import com.lkd.contract.SupplyContract;
import com.lkd.contract.TaskCompleteContract;
import com.lkd.emq.Topic;
import com.lkd.entity.ChannelEntity;
import com.lkd.entity.VendingMachineEntity;
import com.lkd.service.ChannelService;
import com.lkd.service.VendingMachineService;
import com.lkd.vo.VmVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

@Topic("server/vms/supply")
@Service
@Slf4j
public class OperaterMsgHandler implements MsgHandler {

    @Autowired
    private VendingMachineService vendingMachineService;

    @Autowired
    private ChannelService channelService;

    @Override
    public void process(String jsonMsg) throws IOException {
        log.info("处理消息：{}", jsonMsg);
        // List<SupplyContract> supplyContracts = JSON.parseArray(jsonMsg, SupplyContract.class);
        SupplyContract supplyContract = JSON.parseObject(jsonMsg, SupplyContract.class);
        String innerCode = supplyContract.getInnerCode();
        List<SupplyChannel> supplyChannelList = supplyContract.getSupplyData();
        for (SupplyChannel supplyChannel : supplyChannelList) {
            VmVO vmVO = vendingMachineService.findByInnerCode(innerCode);
            VendingMachineEntity vendingMachineEntity = new VendingMachineEntity();
            BeanUtils.copyProperties(vmVO, vendingMachineEntity);
            vendingMachineEntity.setLastSupplyTime(LocalDateTime.now());
            vendingMachineService.updateById(vendingMachineEntity);
            Integer capacity = supplyChannel.getCapacity();
            String channelId = supplyChannel.getChannelId();
            ChannelEntity channelEntity = channelService.getById(channelId);
            channelEntity.setCurrentCapacity(channelEntity.getCurrentCapacity() + capacity);
            channelEntity.setLastSupplyTime(LocalDateTime.now());
            channelEntity.setUpdateTime(LocalDateTime.now());
            channelService.updateById(channelEntity);
        }
    }
}
