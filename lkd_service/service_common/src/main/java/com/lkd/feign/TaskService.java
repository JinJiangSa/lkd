package com.lkd.feign;

import com.lkd.vo.UserWorkVO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "task-service")
public interface TaskService {
    @GetMapping("/task/userWork")
    UserWorkVO userWork(@RequestParam(required = false) Integer userId, @RequestParam String start, @RequestParam String end);
}
