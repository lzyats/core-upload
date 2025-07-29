package com.platform.common.upload.vo;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 文件上传
 */
@Data
@Accessors(chain = true) // 链式调用
public class UploadFileVo {

    /**
     * 文件名称
     */
    private String fileName;
    /**
     * 文件名称
     */
    private String fileKey;
    /**
     * 文件地址
     */
    private String filePath;

}
