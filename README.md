###### 1、maven修改settings.xml，增加如下配置
```
<mirror>
    <id>q3z3-boot-tools-maven</id>
    <name>maven</name>
    <mirrorOf>q3z3-boot-tools-maven</mirrorOf>
    <url>https://q3z3-maven.pkg.coding.net/repository/boot-tools/maven/</url>
</mirror>
```

###### 2、pom.xml增加依赖
```
<!-- 工具包 start -->
<dependency>
    <groupId>com.platform</groupId>
    <artifactId>core-upload</artifactId>
    <version>1.0.0</version>
</dependency>
<!-- 工具包 end -->
```