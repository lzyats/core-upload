package com.platform.common.upload.service;

import cn.hutool.core.lang.Dict;
import com.platform.common.upload.vo.UploadFileVo;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * 上传服务
 */
public interface UploadService {

    /**
     * 获取服务端域名
     */
    String getServerUrl();

    /**
     * 获取上传凭证
     */
    Dict getFileToken();

    /**
     * 文件上传
     */
    UploadFileVo uploadFile(MultipartFile file);

    /**
     * 文件上传
     */
    UploadFileVo uploadFile(File file);

    /**
     * 获取文件流
     */
    InputStream getInputStream(String urlPath);

    /**
     * 删除本地文件
     */
    boolean delFile(File file);

    /**
     * 删除本地文件
     */
    boolean delFile(List<String> dataList);
}
