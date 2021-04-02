package com.clacjz.service.controller;

import com.clacjz.service.bean.ResponseCodePropertyConfig;
import com.clacjz.service.bean.ResponseEntity;
import com.clacjz.service.constant.ResponseConstant;
import com.clacjz.service.process.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * 数据采集服务器的控制器
 * http://localhost:8686/controller/sendData/carInfo
 * 数据的具体内容通过请求头传入
 */
@RestController
@RequestMapping("/controller")
public class DataController {

    @Autowired
    private ResponseCodePropertyConfig config;

    @Autowired
    private DataService service;

    @PostMapping("/sendData/{dataType}")
    public Object collect(@PathVariable("dataType") String dataType , HttpServletRequest request ) throws  Exception{
        service.process(dataType,request);
        return new ResponseEntity(ResponseConstant.CODE_0000,config.getMsg(ResponseConstant.CODE_0000),dataType);
    }
}
