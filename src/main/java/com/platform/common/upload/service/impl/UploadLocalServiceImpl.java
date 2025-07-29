package com.platform.common.upload.service.impl;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import cn.hutool.core.util.IdUtil;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.vo.UploadFileVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

/**
 * 本地上传
 */
@Slf4j
@Service("uploadLocalService")
@Configuration
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "local")
public class UploadLocalServiceImpl extends UploadBaseService implements UploadService {

    @Value("${platform.rootPath}")
    private String rootPath;

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
        String fileName = getFileName(file);
        String fileKey = _getFileKey(rootPath);
        try {
            // 文件拷贝
            file.transferTo(new File(rootPath + FileNameUtil.UNIX_SEPARATOR + fileKey));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        }
        // 组装对象
        String filePath = serverUrl + DEFAULT_DIR + FileNameUtil.UNIX_SEPARATOR + fileKey;
        UploadFileVo fileVo = format(fileName, serverUrl, fileKey)
                .setFilePath(filePath);
        return fileVo;
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        String fileName = getFileName(file);
        String fileKey = _getFileKey(rootPath);
        // 文件拷贝
        FileUtil.copyFile(file, new File(rootPath + FileNameUtil.UNIX_SEPARATOR + fileKey));
        // 组装对象
        UploadFileVo fileVo = format(fileName, serverUrl, fileKey)
                .setFilePath(serverUrl + DEFAULT_DIR + FileNameUtil.UNIX_SEPARATOR + fileKey);
        return fileVo;
    }

    @Override
    public boolean delFile(List<String> dataList) {
        return false;
    }

    /**
     * 本地存储
     */
    private static String _getFileKey(String uploadPath) {
        // 文件路径
        String filePath = DateUtil.format(DateUtil.date(), "yyyy/MM/dd");
        // 生成文件夹
        FileUtil.mkdir(uploadPath + FileNameUtil.UNIX_SEPARATOR + filePath);
        return filePath + FileNameUtil.UNIX_SEPARATOR + IdUtil.objectId();
    }

}
