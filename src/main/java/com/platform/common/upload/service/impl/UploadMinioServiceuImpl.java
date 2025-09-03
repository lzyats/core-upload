package com.platform.common.upload.service.impl;

import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadServiceu;
import com.platform.common.upload.vo.UploadFileVo;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.http.Method;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MinIO 上传
 */
@Slf4j
@Service("uploadMinioServiceu")
@Configuration
@ConditionalOnProperty(prefix = "uploadu", name = "uploadType", havingValue = "minio")
public class UploadMinioServiceuImpl extends UploadBaseService implements UploadServiceu {

    /**
     * 服务端域名
     */
    @Value("${uploadu.serverUrl}")
    private String serverUrl;

    /**
     * accessKey
     */
    @Value("${uploadu.accessKey}")
    private String accessKey;

    /**
     * secretKey
     */
    @Value("${uploadu.secretKey}")
    private String secretKey;

    /**
     * bucket
     */
    @Value("${uploadu.bucket}")
    private String bucket;

    /**
     * prefix
     */
    @Value("${uploadu.prefix}")
    private String prefix;

    // 预签名URL的有效期（例如：30分钟）
    private static final int URL_EXPIRY_MINUTES = 30;

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
    public Dict getFileToken(String fileExt) {
        MinioClient client = initMinio();
        // 1. 生成文件名和存储路径
        String fileName = getFileName();
        // 如果fileExt不为空，则添加后缀
        if (fileExt != null && !fileExt.trim().isEmpty()) {
            // 处理fileExt可能包含的点号，确保只添加一个点
            String ext = fileExt.startsWith(".") ? fileExt : "." + fileExt;
            fileName += ext;
        }
        String fileKey = getFileKey(prefix, fileName);
        try {
            // 计算URL过期时间
            ZonedDateTime expiryTime = ZonedDateTime.now().plusMinutes(URL_EXPIRY_MINUTES);
            // 生成预签名的PUT请求URL，用于客户端直接上传
            String uploadUrl = client.getPresignedObjectUrl(
                    io.minio.GetPresignedObjectUrlArgs.builder()
                            .method(Method.PUT)
                            .bucket(bucket)
                            .object(fileKey)
                            .expiry(URL_EXPIRY_MINUTES, TimeUnit.MINUTES)
                            .build());

            // 8. 返回参数
            return Dict.create()
                    .set("uploadType", UploadTypeEnum.MINIO)
                    .set("serverUrl", uploadUrl)
                    .set("fileKey", fileKey)
                    .set("filePath", serverUrl + "/" + bucket + "/" + fileKey);
        } catch (Exception e) {
            log.error("生成MinIO预签名上传URL失败", e);
            throw new RuntimeException("生成文件上传凭证失败");
        }
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