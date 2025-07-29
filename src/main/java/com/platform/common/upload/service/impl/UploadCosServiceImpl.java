package com.platform.common.upload.service.impl;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.vo.UploadFileVo;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.region.Region;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.List;

/**
 * 腾讯云上传
 */
@Slf4j
@Service("uploadCosService")
@Configuration
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "cos")
public class UploadCosServiceImpl extends UploadBaseService implements UploadService {

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
     * region
     */
    @Value("${upload.region}")
    private String region;

    /**
     * 初始化cos
     */
    private COSClient initCOS() {
        return new COSClient(new BasicCOSCredentials(accessKey, secretKey),
                new com.qcloud.cos.ClientConfig(new Region(region)));
    }

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Dict getFileToken() {
        String fileName = getFileName();
        String fileKey = getFileKey(prefix, fileName);
        String fileHost;
        COSClient client = null;
        try {
            client = initCOS();
            // 这里设置签名在半个小时后过期
            Date expired = DateUtil.offset(DateUtil.date(), DateField.MINUTE, 30);
            // 生成预览URL地址
            URL url = client.generatePresignedUrl(bucket, fileKey, expired, HttpMethodName.PUT);
            fileHost = url.toString();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
        return Dict.create()
                .set("uploadType", UploadTypeEnum.COS)
                .set("serverUrl", fileHost)
                .set("filePath", serverUrl + FileNameUtil.UNIX_SEPARATOR + fileKey);
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        String fileName = getFileName(file);
        String fileKey = getFileKey(prefix);
        fileKey=appendFileExtension(fileName,fileKey);
        // 3 生成 cos 客户端。
        COSClient client = null;
        try {
            client = initCOS();
            //上传到腾讯云
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket
                    , fileKey, file.getInputStream(), new ObjectMetadata());
            client.putObject(putObjectRequest);
            return format(fileName, serverUrl, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        String fileName = getFileName(file);
        String fileKey = getFileKey(prefix);
        fileKey=appendFileExtension(fileName,fileKey);
        InputStream inputStream = FileUtil.getInputStream(file);
        // 3 生成 cos 客户端。
        COSClient client = null;
        try {
            client = initCOS();
            //上传到腾讯云
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket
                    , fileKey, inputStream, new ObjectMetadata());
            client.putObject(putObjectRequest);
            return format(fileName, serverUrl, fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public boolean delFile(List<String> dataList) {
        COSClient client = null;
        try {
            client = initCOS();
            // 生成预览URL地址
            DeleteObjectsRequest deleteObjectRequest = new DeleteObjectsRequest(bucket);
            deleteObjectRequest.withKeys(dataList.toArray(new String[0]));
            client.deleteObjects(deleteObjectRequest);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
        return false;
    }

}
