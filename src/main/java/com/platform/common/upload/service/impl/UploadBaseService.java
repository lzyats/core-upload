package com.platform.common.upload.service.impl;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.RandomUtil;
import com.platform.common.upload.vo.UploadFileVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 基础上传
 */
@Slf4j
public class UploadBaseService {

    protected static final String DEFAULT_DIR = FileNameUtil.UNIX_SEPARATOR + "file";

    /**
     * 获取文件名称
     */
    protected static String getFileName(MultipartFile file) {
        String fileName = file.getOriginalFilename();
        if (StringUtils.isEmpty(fileName)) {
            fileName = IdUtil.objectId();
        }
        return fileName;
    }

    /**
     * 获取文件名称
     */
    protected static String getFileName(File file) {
        String fileName = file.getName();
        if (StringUtils.isEmpty(fileName)) {
            fileName = IdUtil.objectId();
        }
        return fileName;
    }

    /**
     * 获取文件名称
     */
    protected static String getFileName() {
        return IdUtil.objectId();
    }

    /**
     * 获取文件全名
     */
    protected static String getFileKey(String prefix) {
        String fileName = getFileName();
        return getFileKey(prefix, fileName);
    }

    /**
     * 获取文件全名
     */
    protected static String getFileKey(String prefix, String fileName) {
        // 获取当前UTC时间并转换为东8区时间
        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("Asia/Shanghai"));
        DateTimeFormatter yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM");
        DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("dd");
        DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("HH");

        StringBuilder builder = new StringBuilder();
        if (!StringUtils.isEmpty(prefix)) {
            builder.append(prefix);
            builder.append(FileNameUtil.UNIX_SEPARATOR);
        }
        builder.append(zonedDateTime.format(yearMonthFormatter))
                .append(FileNameUtil.UNIX_SEPARATOR)
                .append(zonedDateTime.format(dayFormatter))
                .append(FileNameUtil.UNIX_SEPARATOR)
                .append(zonedDateTime.format(hourFormatter))
                .append(FileNameUtil.UNIX_SEPARATOR);
        return builder.toString() + fileName;
    }

    /**
     * 获取文件流
     */
    public InputStream getInputStream(String urlPath) {
        try {
            URL url = new URL(urlPath);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            // 设置网络连接超时时间
            httpURLConnection.setConnectTimeout(5000);
            // 设置应用程序要从网络连接读取数据
            httpURLConnection.setDoInput(true);
            // 从服务器返回一个输入流
            return httpURLConnection.getInputStream();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("获取文件流失败");
        }
    }

    /**
     * 封装对象
     */
    protected static UploadFileVo format(String fileName, String serverUrl, String fileKey) {
        // 服务器地址
        return new UploadFileVo()
                .setFileName(fileName)
                .setFileKey(fileKey)
                .setFilePath(serverUrl + FileNameUtil.UNIX_SEPARATOR + fileKey);
    }

    /**
     * 处理文件名后缀并添加到fileKey
     * @param fileName 原始文件名
     * @param fileKey 原始文件键
     * @return 添加了后缀的fileKey
     */
    protected static String appendFileExtension(String fileName, String fileKey) {
        String fileExtension = "";
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex != -1 && dotIndex < fileName.length() - 1) {
            fileExtension = fileName.substring(dotIndex);
        }

        if (!fileExtension.isEmpty()) {
            fileKey = fileKey + fileExtension;
        }

        return fileKey;
    }

    /**
     * 删除本地文件
     */
    public boolean delFile(File file) {
        try {
            return FileUtil.del(file);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("删除上传失败");
        }
    }

}
