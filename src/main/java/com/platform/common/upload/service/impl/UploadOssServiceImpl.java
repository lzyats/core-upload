package com.platform.common.upload.service.impl;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.PolicyConditions;
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
import java.io.InputStream;
import java.util.Date;
import java.util.List;

/**
 * 阿里云上传
 */
@Slf4j
@Service("uploadOssService")
@Configuration
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "oss")
public class UploadOssServiceImpl extends UploadBaseService implements UploadService {

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
     * region
     */
    @Value("${upload.region}")
    private String region;
    /**
     * prefix
     */
    @Value("${upload.prefix}")
    private String prefix;

    /**
     * 初始化oss
     */
    private OSS initOSS() {
        return new OSSClientBuilder()
                .build(region, accessKey, secretKey);
    }

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Dict getFileToken() {
        // 1、默认固定值，分钟
        Integer expire = 30;
        // 2、过期时间
        Date expiration = DateUtil.offsetMinute(DateUtil.date(), expire);
        // 3、构造“策略”（Policy）
        OSS ossClient = initOSS();
        PolicyConditions policyConditions = new PolicyConditions();
        policyConditions.addConditionItem(PolicyConditions.COND_CONTENT_LENGTH_RANGE, 0, 1048576000);
        String policy = ossClient.generatePostPolicy(expiration, policyConditions);
        String signature = ossClient.calculatePostSignature(policy);
        // 4、文件名称
        String fileName = getFileName();
        String fileKey = getFileKey(prefix, fileName);
        return Dict.create()
                .set("uploadType", UploadTypeEnum.OSS)
                .set("serverUrl", serverUrl)
                .set("accessKey", accessKey)
                .set("policy", Base64.encode(policy))
                .set("signature", signature)
                .set("fileKey", fileKey)
                .set("filePath", serverUrl + FileNameUtil.UNIX_SEPARATOR + fileKey);
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        OSS client = initOSS();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);
            fileKey=appendFileExtension(fileName,fileKey);
            client.putObject(bucket, fileKey, file.getInputStream());
            return format(fileName, serverUrl, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            client.shutdown();
        }
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        OSS client = initOSS();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);
            fileKey=appendFileExtension(fileName,fileKey);
            InputStream inputStream = FileUtil.getInputStream(file);
            client.putObject(bucket, fileKey, inputStream);
            return format(fileName, serverUrl, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            client.shutdown();
        }
    }

    @Override
    public boolean delFile(List<String> dataList) {
        return false;
    }

}
