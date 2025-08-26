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
import com.platform.common.upload.service.UploadServiceu;
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
@Service("uploadOssServiceu")
@Configuration
@ConditionalOnProperty(prefix = "uploadu", name = "uploadType", havingValue = "oss")
public class UploadOssServiceuImpl extends UploadBaseService implements UploadServiceu {

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
     * region
     */
    @Value("${uploadu.region}")
    private String region;
    /**
     * prefix
     */
    @Value("${uploadu.prefix}")
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
    public Dict getFileToken(String fileExt) {
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
        // 如果fileExt不为空，则添加后缀
        if (fileExt != null && !fileExt.trim().isEmpty()) {
            // 处理fileExt可能包含的点号，确保只添加一个点
            String ext = fileExt.startsWith(".") ? fileExt : "." + fileExt;
            fileName += ext;
        }
        String fileKey = getFileKey(prefix, fileName);
        //log.info("声音文件：{},{}",fileKey,serverUrl + FileNameUtil.UNIX_SEPARATOR + fileKey);
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
