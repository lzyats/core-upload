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
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.digest.HMac;
import cn.hutool.crypto.digest.HmacAlgorithm;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.digest.HMac;
import cn.hutool.crypto.digest.HmacAlgorithm;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
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
        try {
            // 1. 生成文件名和存储路径
            String fileName = getFileName();
            String fileKey = getFileKey(prefix, fileName);

            // 2. 设置过期时间（30分钟后，UTC时间）
            Date expiration = new Date(System.currentTimeMillis() + 30 * 60 * 1000);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String expirationStr = sdf.format(expiration);

            // 3. 使用 Jackson 构建 Policy 文档（替代 fastjson）
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode policyObj = mapper.createObjectNode();
            policyObj.put("expiration", expirationStr); // 过期时间

            ArrayNode conditions = mapper.createArrayNode();
            // 限制存储桶
            ArrayNode bucketCondition = mapper.createArrayNode();
            bucketCondition.add("eq").add("$bucket").add(bucket);
            conditions.add(bucketCondition);

            // 限制文件路径（key）
            ArrayNode keyCondition = mapper.createArrayNode();
            keyCondition.add("eq").add("$key").add(fileKey);
            conditions.add(keyCondition);

            // 限制文件大小（0-1GB）
            ArrayNode sizeCondition = mapper.createArrayNode();
            sizeCondition.add("content-length-range").add(0).add(1024 * 1024 * 1024);
            conditions.add(sizeCondition);

            policyObj.set("conditions", conditions);

            // 4. 对 Policy 进行 Base64 编码
            String policyJson = mapper.writeValueAsString(policyObj);
            String encodedPolicy = Base64.getEncoder().encodeToString(policyJson.getBytes(StandardCharsets.UTF_8));

            // 5. 使用 secretKey 对 encodedPolicy 进行 HMAC-SHA1 签名
            HMac hmac = SecureUtil.hmac(HmacAlgorithm.HmacSHA1, secretKey.getBytes(StandardCharsets.UTF_8));
            String signature = hmac.digestHex(encodedPolicy);

            // 6. 返回表单所需参数
            return Dict.create()
                    .set("uploadType", UploadTypeEnum.MINIO)
                    .set("serverUrl", serverUrl + "/" + bucket) // 表单提交地址
                    .set("accessKey", accessKey)
                    .set("policy", encodedPolicy)
                    .set("signature", signature)
                    .set("fileKey", fileKey)
                    .set("filePath", serverUrl + "/" + bucket + "/" + fileKey);
        } catch (JsonProcessingException e) {
            log.error("生成MinIO Policy JSON失败", e);
            throw new RuntimeException("生成文件上传凭证失败");
        } catch (Exception e) {
            log.error("生成MinIO表单上传凭证失败", e);
            throw new RuntimeException("生成文件上传凭证失败");
        }
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        MinioClient client = initMinio();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);

            //fileKey=appendFileExtension(fileName,fileKey);

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