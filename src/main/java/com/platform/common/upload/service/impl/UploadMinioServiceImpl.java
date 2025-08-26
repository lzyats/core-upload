package com.platform.common.upload.service.impl;

import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.lang.Dict;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.platform.common.upload.enums.UploadTypeEnum;
import com.platform.common.upload.service.UploadService;
import com.platform.common.upload.vo.UploadFileVo;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.HmacUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
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

            String region = "us-east-1"; // MinIO 区域（需与实际配置一致）

            // 2. 时间参数（AWS4 签名核心要素）
            ZonedDateTime utcNow = ZonedDateTime.now(ZoneId.of("UTC"));
            String amzDate = utcNow.format(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'"));
            String dateStamp = utcNow.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

            // 3. AWS4 签名基础参数
            String algorithm = "AWS4-HMAC-SHA256";
            String credentialScope = String.format("%s/%s/s3/aws4_request", dateStamp, region);
            String amzCredential = String.format("%s/%s", accessKey, credentialScope);

            // 4. 构建 Policy 文档
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode policyObj = mapper.createObjectNode();
            policyObj.put("expiration", utcNow.plusMinutes(30).format(DateTimeFormatter.ISO_INSTANT));

            ArrayNode conditions = mapper.createArrayNode();
            conditions.add(mapper.createArrayNode().add("eq").add("$bucket").add(bucket));
            conditions.add(mapper.createArrayNode().add("eq").add("$key").add(fileKey));
            conditions.add(mapper.createArrayNode().add("content-length-range").add(0).add(1024 * 1024 * 1024));
            conditions.add(mapper.createArrayNode().add("eq").add("$x-amz-algorithm").add(algorithm));
            conditions.add(mapper.createArrayNode().add("eq").add("$x-amz-credential").add(amzCredential));
            conditions.add(mapper.createArrayNode().add("eq").add("$x-amz-date").add(amzDate));

            policyObj.set("conditions", conditions);

            // 5. Policy 编码
            String policyJson = mapper.writeValueAsString(policyObj);
            String encodedPolicy = Base64.getEncoder().encodeToString(policyJson.getBytes(StandardCharsets.UTF_8));

            // 6. 生成 AWS4 签名密钥（关键修正：字符串转字节数组）
            byte[] kSecret = ("AWS4" + secretKey).getBytes(StandardCharsets.UTF_8);
            byte[] kDate = HmacUtils.hmacSha256(kSecret, dateStamp.getBytes(StandardCharsets.UTF_8)); // 字符串转字节数组
            byte[] kRegion = HmacUtils.hmacSha256(kDate, region.getBytes(StandardCharsets.UTF_8)); // 字符串转字节数组
            byte[] kService = HmacUtils.hmacSha256(kRegion, "s3".getBytes(StandardCharsets.UTF_8)); // 字符串转字节数组
            byte[] signingKey = HmacUtils.hmacSha256(kService, "aws4_request".getBytes(StandardCharsets.UTF_8)); // 字符串转字节数组

            // 7. 计算签名（关键修正：字符串转字节数组）
            String stringToSign = String.format("%s\n%s\n%s\n%s",
                    algorithm,
                    amzDate,
                    credentialScope,
                    DigestUtils.sha256Hex(encodedPolicy));

            byte[] stringToSignBytes = stringToSign.getBytes(StandardCharsets.UTF_8); // 字符串转字节数组
            String signature = Hex.encodeHexString(HmacUtils.hmacSha256(signingKey, stringToSignBytes));
            log.info("x-amz-credential: {}", amzCredential);

            // 8. 返回参数
            return Dict.create()
                    .set("uploadType", UploadTypeEnum.MINIO)
                    .set("serverUrl", serverUrl + "/" + bucket)
                    .set("x-amz-algorithm", algorithm)
                    .set("x-amz-date", amzDate)
                    .set("x-amz-credential", amzCredential)
                    .set("policy", encodedPolicy)
                    .set("signature", signature)
                    .set("fileKey", fileKey)
                    .set("accessKey", accessKey)
                    .set("filePath", serverUrl + "/" + bucket + "/" + fileKey)
                    .set("region", region);
        } catch (JsonProcessingException e) {
            log.error("生成MinIO Policy JSON失败", e);
            throw new RuntimeException("生成文件上传凭证失败");
        } catch (Exception e) {
            log.error("生成MinIO AWS4签名失败", e);
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