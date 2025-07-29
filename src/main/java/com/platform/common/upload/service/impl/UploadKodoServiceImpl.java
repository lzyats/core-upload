package com.platform.common.upload.service.impl;

import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.vo.UploadFileVo;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.Auth;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

/**
 * 七牛云上传
 */
@Slf4j
@Service("uploadKodoService")
@Configuration
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "kodo")
public class UploadKodoServiceImpl extends UploadBaseService implements UploadService {

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
    @Value("${upload.prefix:}")
    private String prefix;

    /**
     * 获取Auth
     */
    private Auth getAuth() {
        return Auth.create(accessKey, secretKey);
    }

    /**
     * 获取Token
     */
    private String getToken(String fileKey) {
        return getAuth().uploadToken(bucket, fileKey);
    }

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Dict getFileToken() {
        String fileName = getFileName();
        String fileKey = getFileKey(prefix, fileName);
        String token = getToken(fileKey);
        return Dict.create()
                .set("uploadType", UploadTypeEnum.KODO)
                .set("serverUrl", region)
                .set("fileKey", fileKey)
                .set("fileToken", token)
                .set("filePath", serverUrl + FileNameUtil.UNIX_SEPARATOR + fileKey);
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        String fileName = getFileName(file);
        String fileKey = getFileKey(prefix);
        fileKey=appendFileExtension(fileName,fileKey);
        String token = getToken(fileKey);
        Response response = null;
        try {
            UploadManager uploadManager = new UploadManager(new com.qiniu.storage.Configuration());
            response = uploadManager.put(file.getBytes(), fileKey, token);
            return format(fileName, serverUrl, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        String fileName = getFileName(file);
        String fileKey = getFileKey(prefix);
        fileKey=appendFileExtension(fileName,fileKey);
        String token = getToken(fileKey);
        Response response = null;
        try {
            UploadManager uploadManager = new UploadManager(new com.qiniu.storage.Configuration());
            response = uploadManager.put(file, fileKey, token);
            return format(fileName, serverUrl, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    public boolean delFile(List<String> dataList) {
        Auth auth = getAuth();
        BucketManager bucketManager = new BucketManager(auth, new com.qiniu.storage.Configuration());
        BucketManager.BatchOperations operations = new BucketManager.BatchOperations();
        dataList.forEach(data -> {
            operations.addDeleteOp(bucket, data);
        });
        try {
            bucketManager.batch(operations);
        } catch (QiniuException ex) {
            //如果遇到异常，说明删除失败
            System.err.println(ex.code());
            System.err.println(ex.response.toString());
        }
        return false;
    }

}
