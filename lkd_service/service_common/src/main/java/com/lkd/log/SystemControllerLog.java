package com.lkd.log;

import java.lang.annotation.*;

/**
 * @Author: yinshijin
 * @Date: 2022/9/21
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SystemControllerLog {
}
