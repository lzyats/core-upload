package com.platform.common.upload.service.impl;

import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.vo.UploadFileVo;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * MinIO 上传
 */
@Slf4j
@Service("uploadMinioService")
@Configuration
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "minio")
public class UploadMinioServiceImpl extends UploadBaseService implements UploadService {

    /**
     * 服务端域名
     */
    @Value("${upload.serverUrl}")
    private String serverUrl;

    /**
     * accessKey
     */
    @Value("${upload.accessKey}")
    private String accessKey;

    /**
     * secretKey
     */
    @Value("${upload.secretKey}")
    private String secretKey;

    /**
     * bucket
     */
    @Value("${upload.bucket}")
    private String bucket;

    /**
     * prefix
     */
    @Value("${upload.prefix}")
    private String prefix;

    /**
     * 初始化 MinIO 客户端
     */
    private MinioClient initMinio() {
        return MinioClient.builder()
                .endpoint(serverUrl)
                .credentials(accessKey, secretKey)
                .build();
    }

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Dict getFileToken() {
        String fileName = getFileName();
        String fileKey = getFileKey(prefix, fileName);
        return Dict.create()
                .set("uploadType", UploadTypeEnum.MINIO)
                .set("serverUrl", serverUrl)
                .set("bucket", bucket)
                .set("fileKey", fileKey)
                .set("filePath", serverUrl + FileNameUtil.UNIX_SEPARATOR + bucket + FileNameUtil.UNIX_SEPARATOR + fileKey);
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        MinioClient client = initMinio();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);

            fileKey=appendFileExtension(fileName,fileKey);

            InputStream inputStream = file.getInputStream();
            client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(fileKey)
                    .stream(inputStream, inputStream.available(), -1)
                    .build());
            return format(fileName, serverUrl + FileNameUtil.UNIX_SEPARATOR + bucket, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        }
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        MinioClient client = initMinio();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);

            fileKey=appendFileExtension(fileName,fileKey);

            InputStream inputStream = java.nio.file.Files.newInputStream(file.toPath());
            client.putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(fileKey)
                    .stream(inputStream, inputStream.available(), -1)
                    .build());
            return format(fileName, serverUrl + FileNameUtil.UNIX_SEPARATOR + bucket, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        }
    }

    @Override
    public boolean delFile(List<String> dataList) {
        MinioClient client = initMinio();
        try {
            for (String data : dataList) {
                client.removeObject(io.minio.RemoveObjectArgs.builder()
                        .bucket(bucket)
                        .object(data)
                        .build());
            }
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件删除失败");
        }
    }
}