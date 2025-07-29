package com.platform.common.upload.service.impl;

import cn.hutool.core.lang.Dict;
import com.github.tobato.fastdfs.domain.fdfs.StorePath;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.utils.FastUtils;
import com.platform.common.upload.vo.UploadFileVo;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

/**
 * fast上传
 */
@Slf4j
@Service("uploadFastService")
@Configuration
@NoArgsConstructor
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "fast")
public class UploadFastServiceImpl extends UploadBaseService implements UploadService {

    /**
     * 服务端域名
     */
    @Value("${upload.serverUrl}")
    private String serverUrl;

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Dict getFileToken() {
        return Dict.create()
                .set("uploadType", UploadTypeEnum.LOCAL);
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        StorePath storePath;
        try {
            storePath = FastUtils.uploadFile(file);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        }
        String fileKey = storePath.getFullPath();
        String fileName = getFileName(file);
        return format(fileName, serverUrl, fileKey);
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        StorePath storePath;
        try {
            storePath = FastUtils.uploadFile(file);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        }
        String fileName = getFileName(file);
        String fileKey = storePath.getFullPath();
        return format(fileName, serverUrl, fileKey);
    }

    @Override
    public boolean delFile(List<String> dataList) {
        return false;
    }

}
