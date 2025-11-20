package com.platform.common.upload.service.impl;

import cn.hutool.core.lang.Dict;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.vo.UploadFileVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;




import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.List;

/**
 * AWS S3 上传实现
 */
@Slf4j
@Service("uploadAwsService")
@Configuration
@ConditionalOnProperty(prefix = "upload", name = "uploadType", havingValue = "aws")
public class UploadAwsServiceImpl extends UploadBaseService implements UploadService {

    /**
     * AWS 区域
     */
    @Value("${upload.region:us-east-1}")
    private String region;

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
     * CloudFront
     */
    @Value("${uploadu.cloudfront}")
    private String cf;

    // 预签名URL的有效期（例如：30分钟）
    private static final int URL_EXPIRY_MINUTES = 30;

    /**
     * 初始化 S3 客户端
     */
    private S3Client initS3Client() {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }

    @Override
    public String getServerUrl() {
        return "https://" + bucket + ".s3." + region + ".amazonaws.com";
    }

    @Override
    public Dict getFileToken(String fileExt) {
        try {
            // 1. 生成文件名和存储路径
            String fileName = getFileName();
            // 如果fileExt不为空，则添加后缀
            if (fileExt != null && !fileExt.trim().isEmpty()) {
                // 处理fileExt可能包含的点号，确保只添加一个点
                String ext = fileExt.startsWith(".") ? fileExt : "." + fileExt;
                fileName += ext;
            }
            String fileKey = getFileKey(prefix, fileName);

            // 生成预签名的PUT请求URL，用于客户端直接上传
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(fileKey)
                    .build();

            S3Presigner presigner = S3Presigner.builder()
                    .region(Region.of(region))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(accessKey, secretKey)))
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(URL_EXPIRY_MINUTES))
                    .putObjectRequest(objectRequest)
                    .build();

            // 使用正确的类 PresignedPutObjectRequest
            PresignedPutObjectRequest presignedResponse = presigner.presignPutObject(presignRequest);
            presigner.close();

            URL uploadUrl = presignedResponse.url();

            //生成 CloudFront 地址
            String cloudFrontUrl = "https://" + cf + "/" + fileKey;

            // 返回参数
            return Dict.create()
                    .set("uploadType", UploadTypeEnum.AWS)
                    .set("serverUrl", uploadUrl.toString())
                    .set("fileKey", fileKey)
                    .set("cloudFrontUrl", cloudFrontUrl)
                    .set("filePath", getServerUrl() + "/" + fileKey);
        } catch (Exception e) {
            log.error("生成AWS S3预签名上传URL失败", e);
            throw new RuntimeException("生成文件上传凭证失败");
        }
    }

    @Override
    public UploadFileVo uploadFile(MultipartFile file) {
        S3Client client = initS3Client();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);
            fileKey = appendFileExtension(fileName, fileKey);

            InputStream inputStream = file.getInputStream();

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(fileKey)
                    .build();

            client.putObject(putObjectRequest, software.amazon.awssdk.core.sync.RequestBody.fromInputStream(
                    inputStream, inputStream.available()));

            return format(fileName, getServerUrl(), fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            client.close();
        }
    }

    @Override
    public UploadFileVo uploadFile(File file) {
        S3Client client = initS3Client();
        try {
            String fileName = getFileName(file);
            String fileKey = getFileKey(prefix);
            fileKey = appendFileExtension(fileName, fileKey);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(fileKey)
                    .build();

            client.putObject(putObjectRequest, software.amazon.awssdk.core.sync.RequestBody.fromFile(file.toPath()));

            return format(fileName, getServerUrl(), fileKey);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件上传失败");
        } finally {
            client.close();
        }
    }

    @Override
    public boolean delFile(List<String> dataList) {
        S3Client client = initS3Client();
        try {
            for (String data : dataList) {
                DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(data)
                        .build();
                client.deleteObject(deleteObjectRequest);
            }
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("文件删除失败");
        } finally {
            client.close();
        }
    }
}
