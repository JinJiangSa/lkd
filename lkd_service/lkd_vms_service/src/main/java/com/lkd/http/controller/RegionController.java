package com.lkd.http.controller;
import com.lkd.entity.RegionEntity;

import com.lkd.http.vo.RegionReq;
import com.lkd.service.RegionService;
import com.lkd.vo.Pager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/region")
public class RegionController {
    @Autowired
    private RegionService regionService;

    /**
     * 分页搜索
     * @param pageIndex
     * @param pageSize
     * @param name
     * @return
     */
    @GetMapping("/search")
    Pager<RegionEntity> search(
            @RequestParam(value = "pageIndex",required = false,defaultValue = "1") Long pageIndex,
            @RequestParam(value = "pageSize",required = false,defaultValue = "10") Long pageSize,
            @RequestParam(value = "name",required = false,defaultValue = "") String name){

        return regionService.search(pageIndex,pageSize,name);
    }

    /**
     * 添加区域
     * @param regionReq
     * @return
     */
    @PostMapping
    public boolean add(@RequestBody RegionReq regionReq){
        RegionEntity region=new RegionEntity();
        region.setName(regionReq.getRegionName());
        region.setRemark(regionReq.getRemark());
        return regionService.save(region);
    }

    /**
     * 更新区域
     * @param id
     * @param req
     * @return
     */
    @PutMapping("/{id}")
    public boolean update(@PathVariable String id,@RequestBody RegionReq req){
        RegionEntity region = regionService.getById(Long.valueOf(id));
        if(region == null) return false;
        region.setName(req.getRegionName());
        region.setRemark(req.getRemark());
        return regionService.updateById(region);
    }

    /**
     * 获取区域详情
     * @param regionId
     * @return
     */
    @GetMapping("/{regionId}")
    public RegionEntity findById(@PathVariable String regionId){
        return regionService.getById(Long.valueOf(regionId));
    }

    /**
     * 删除区域
     * @param regionId
     * @return
     */
    @DeleteMapping("/{regionId}")
    public Boolean delete(@PathVariable String regionId){
        return regionService.removeById(Long.valueOf(regionId));
    }
}
