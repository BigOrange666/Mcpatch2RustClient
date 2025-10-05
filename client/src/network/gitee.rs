use std::ops::Range;
use std::pin::Pin;
use std::path::PathBuf;

use async_trait::async_trait;
use reqwest::Client;
use reqwest::ClientBuilder;
use serde_json::Value;
use tokio::io::AsyncRead;
use tokio::time::Duration;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use crate::error::BusinessError;
use crate::global_config::GlobalConfig;
use crate::network::DownloadResult;
use crate::network::UpdatingSource;

/// 代表gitee更新协议
pub struct GiteeProtocol {
    /// gitee api的url
    pub api_url: String,

    /// http客户端对象
    pub client: Client,

    /// 打码的关键字，所有日志里的这个关键字都会被打码
    mask_keyword: String,

    /// 缓存的完整文件路径
    cached_files: std::collections::HashMap<String, (PathBuf, std::time::SystemTime)>,
}

impl GiteeProtocol {
    pub fn new(api_url: &str, config: &GlobalConfig, _index: u32) -> Self {
        let client = ClientBuilder::new()
            .connect_timeout(Duration::from_millis(config.http_timeout as u64))
            .read_timeout(Duration::from_millis(config.http_timeout as u64))
            .danger_accept_invalid_certs(config.http_ignore_certificate)
            .build()
            .unwrap();

        let mask_keyword = match reqwest::Url::parse(api_url) {
            Ok(parsed) => {
                let host = parsed.host_str().unwrap_or("gitee.com");
                host.to_owned()
            },
            Err(_) => "gitee.com".to_owned(),
        };

        Self {
            api_url: api_url.to_owned(),
            client,
            mask_keyword,
            cached_files: std::collections::HashMap::new(),
        }
    }

    /// 构造带token的API URL
    fn build_api_url_with_token(&self, base_url: &str, config: &GlobalConfig) -> String {
        let mut url = base_url.to_string();
        if !config.gitee_token.is_empty() {
            if url.contains('?') {
                url.push_str(&format!("&access_token={}", config.gitee_token));
            } else {
                url.push_str(&format!("?access_token={}", config.gitee_token));
            }
        }
        url
    }

    /// 从gitee api响应中获取index.json的实际下载URL，并添加token支持
    async fn get_index_download_url(&self, config: &GlobalConfig) -> Result<String, BusinessError> {
        // 构造带token的API URL
        let api_url = self.build_api_url_with_token(&self.api_url, config);

        eprintln!("请求gitee API URL: {}", api_url);

        // 发送请求获取文件列表
        let rsp = self.client.get(&api_url)
            .send()
            .await
            .map_err(|e| BusinessError::new(format!("请求gitee api失败: {:?}", e)))?;

        let code = rsp.status().as_u16();
        if code < 200 || code >= 300 {
            let body = rsp.text().await.unwrap_or_else(|_| "".to_string());
            eprintln!("gitee API响应状态码 {}: body = {}", code, body);
            return Err(BusinessError::new(format!("gitee api返回错误状态码 {}: {}", code, body)));
        }

        let json_text = rsp.text().await
            .map_err(|e| BusinessError::new(format!("读取gitee api响应失败: {:?}", e)))?;

        eprintln!("gitee API响应内容: {}", json_text);

        // 解析JSON响应
        let json_value: Value = serde_json::from_str(&json_text)
            .map_err(|e| BusinessError::new(format!("解析gitee api响应JSON失败: {:?}", e)))?;

        // 查找index.json的download_url
        if let Some(files) = json_value.as_array() {
            for file in files {
                if let Some(name) = file["name"].as_str() {
                    if name == "index.json" {
                        if let Some(download_url) = file["download_url"].as_str() {
                            let mut final_url = download_url.to_string();
                            // 为下载URL也添加token
                            if !config.gitee_token.is_empty() {
                                if final_url.contains('?') {
                                    final_url.push_str(&format!("&access_token={}", config.gitee_token));
                                } else {
                                    final_url.push_str(&format!("?access_token={}", config.gitee_token));
                                }
                            }
                            eprintln!("找到index.json下载URL: {}", final_url);
                            return Ok(final_url);
                        }
                    }
                }
            }
        }

        Err(BusinessError::new("在gitee api响应中未找到index.json文件"))
    }

    /// 解析gitee API URL获取仓库信息，用于构造v5 API下载地址
    fn parse_gitee_url(&self, api_url: &str) -> Option<(String, String, String, String)> {
        // 解析类似这样的URL: https://gitee.com/api/v5/repos/用户名/仓库名/contents/路径?ref=分支
        let url = reqwest::Url::parse(api_url).ok()?;
        let path_segments: Vec<&str> = url.path_segments()?.collect();
        
        if path_segments.len() < 6 || path_segments[0] != "api" || path_segments[1] != "v5" || path_segments[2] != "repos" {
            return None;
        }
        
        let owner = path_segments[3].to_string();
        let repo = path_segments[4].to_string();
        
        // 提取ref参数
        let mut ref_param = "master".to_string();
        for (key, value) in url.query_pairs() {
            if key == "ref" {
                ref_param = value.to_string();
                break;
            }
        }
        
        // 提取路径部分（从contents之后的部分）
        let path_start = path_segments.iter().position(|&s| s == "contents")?;
        let path_parts: Vec<&str> = path_segments[path_start + 1..].to_vec();
        let path = path_parts.join("/");
        
        Some((owner, repo, path, ref_param))
    }

    /// 构造gitee v5 API的原始文件下载URL
    fn build_raw_download_url(&self, file_path: &str, config: &GlobalConfig) -> Option<String> {
        let (owner, repo, base_path, ref_param) = self.parse_gitee_url(&self.api_url)?;
        
        // 构造完整的文件路径
        let full_path = if base_path.is_empty() {
            file_path.to_string()
        } else {
            format!("{}/{}", base_path, file_path)
        };
        
        // 对路径进行URL编码
        let encoded_path = urlencoding::encode(&full_path);
        
        // 构造原始文件下载URL格式：
        // https://gitee.com/api/v5/repos/{owner}/{repo}/raw/{file_path}?access_token={token}&ref={ref}
        let mut raw_url = format!(
            "https://gitee.com/api/v5/repos/{}/{}/raw/{}",
            owner, repo, encoded_path
        );
        
        // 添加查询参数
        let mut has_params = false;
        if !config.gitee_token.is_empty() {
            raw_url.push_str(&format!("?access_token={}", config.gitee_token));
            has_params = true;
        }
        
        if !ref_param.is_empty() {
            if has_params {
                raw_url.push_str(&format!("&ref={}", ref_param));
            } else {
                raw_url.push_str(&format!("?ref={}", ref_param));
            }
        }
        
        eprintln!("构造的原始下载URL: {}", raw_url);
        Some(raw_url)
    }

    /// 完整下载文件并缓存
    async fn download_and_cache_file(&mut self, file_name: &str, config: &GlobalConfig) -> Result<PathBuf, BusinessError> {
        eprintln!("开始下载文件: {}", file_name);
        
        // 检查是否已缓存且未过期
        if let Some((path, timestamp)) = self.cached_files.get(file_name) {
            let now = std::time::SystemTime::now();
            let duration = now.duration_since(*timestamp).unwrap_or_else(|_| std::time::Duration::from_secs(0));
            // 缓存10分钟内有效
            if duration.as_secs() < 600 && tokio::fs::try_exists(path).await.unwrap_or(false) {
                eprintln!("使用缓存文件: {:?}", path);
                return Ok(path.clone());
            }
        }

        // 为特定文件构建下载URL
        let final_download_url = if file_name == "index.json" {
            // 对于index.json，使用API获取的下载URL
            self.get_index_download_url(config).await?
        } else {
            // 对于其他文件，使用v5 API原始下载URL
            self.build_raw_download_url(file_name, config)
                .ok_or_else(|| BusinessError::new("无法构造文件下载URL"))?
        };

        eprintln!("下载URL: {}", final_download_url);

        // 创建临时目录
        let temp_dir = std::env::temp_dir().join("mcpatch_gitee");
        tokio::fs::create_dir_all(&temp_dir).await
            .map_err(|e| BusinessError::new(format!("创建临时目录失败: {:?}", e)))?;

        // 生成临时文件路径
        let temp_path = temp_dir.join(format!("{}_{}", file_name, Uuid::new_v4()));

        // 开始流式下载文件
        let rsp = self.client.get(&final_download_url)
            .send()
            .await
            .map_err(|e| BusinessError::new(format!("下载文件失败: {:?}", e)))?;

        let code = rsp.status().as_u16();
        if code < 200 || code >= 300 {
            let body = rsp.text().await.unwrap_or_else(|_| "".to_string());
            eprintln!("下载文件返回错误状态码 {}: body = {}", code, body);
            return Err(BusinessError::new(format!("下载文件返回错误状态码 {}: {} - {}", code, final_download_url, body)));
        }

        // 获取文件总大小
        let total_size = rsp.content_length().unwrap_or(0);
        eprintln!("文件总大小: {} bytes", total_size);

        // 创建进度条
        let pb = if total_size > 0 {
            let pb = ProgressBar::new(total_size);
            pb.set_style(ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .expect("模板格式错误")
                .progress_chars("#>-"));
            pb
        } else {
            ProgressBar::new_spinner()
        };

        // 创建临时文件
        let mut file = tokio::fs::File::create(&temp_path).await
            .map_err(|e| BusinessError::new(format!("创建临时文件失败: {:?}", e)))?;

        // 流式下载并显示进度
        let mut stream = rsp.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| BusinessError::new(format!("读取文件数据失败: {:?}", e)))?;
            file.write_all(&chunk).await
                .map_err(|e| BusinessError::new(format!("写入临时文件失败: {:?}", e)))?;
            
            pb.inc(chunk.len() as u64);
        }

        pb.finish_with_message("下载完成");

        file.flush().await
            .map_err(|e| BusinessError::new(format!("刷新文件失败: {:?}", e)))?;

        eprintln!("下载文件大小: {} bytes", total_size);
        
        // 如果是index.json，读取内容用于调试
        if file_name == "index.json" {
            let content = tokio::fs::read(&temp_path).await
                .map_err(|e| BusinessError::new(format!("读取index.json文件失败: {:?}", e)))?;
            let content_str = String::from_utf8_lossy(&content);
            eprintln!("index.json内容: {}", content_str);
        }

        // 缓存文件路径和时间戳
        self.cached_files.insert(file_name.to_owned(), (temp_path.clone(), std::time::SystemTime::now()));

        eprintln!("文件下载完成: {:?}", temp_path);
        Ok(temp_path)
    }

    /// 从缓存文件中提取指定范围的数据
    async fn extract_range_from_file(&self, file_path: &PathBuf, range: &Range<u64>) -> Result<Vec<u8>, BusinessError> {
        eprintln!("从文件提取数据范围: {:?}, start={}, len={}", file_path, range.start, range.end - range.start);
        
        let mut file = tokio::fs::File::open(file_path).await
            .map_err(|e| BusinessError::new(format!("打开缓存文件失败: {:?}", e)))?;

        // 获取文件大小
        let file_metadata = file.metadata().await
            .map_err(|e| BusinessError::new(format!("获取文件元数据失败: {:?}", e)))?;
        let file_size = file_metadata.len();

        let start = range.start;
        
        // 检查起始位置是否超出文件大小
        if start >= file_size {
            eprintln!("起始位置 {} 超出文件大小 {}，返回空数据", start, file_size);
            return Ok(vec![]);
        }

        // 计算实际可读取的长度
        let max_len = (file_size - start) as usize;
        let requested_len = if range.end == u64::MAX {
            usize::MAX // 读取到文件末尾
        } else {
            (range.end - range.start) as usize
        };
        let actual_len = std::cmp::min(max_len, requested_len);

        eprintln!("文件大小: {}, 请求长度: {}, 实际长度: {}", file_size, requested_len, actual_len);

        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(start)).await
            .map_err(|e| BusinessError::new(format!("定位文件位置失败: {:?}", e)))?;

        let mut buffer = vec![0u8; actual_len];
        use tokio::io::AsyncReadExt;
        
        // 使用read_exact只读取实际可读取的数据
        if actual_len > 0 {
            file.read_exact(&mut buffer).await
                .map_err(|e| BusinessError::new(format!("读取文件数据失败: {:?}", e)))?;
        }

        eprintln!("成功提取数据: {} bytes", buffer.len());
        Ok(buffer)
    }

    /// 清理过期的缓存文件
    pub async fn cleanup_cache(&mut self) {
        let mut to_remove = Vec::new();
        let now = std::time::SystemTime::now();

        for (file_name, (path, timestamp)) in &self.cached_files {
            let duration = now.duration_since(*timestamp).unwrap_or_else(|_| std::time::Duration::from_secs(0));
            if duration.as_secs() >= 600 || !tokio::fs::try_exists(path).await.unwrap_or(false) {
                to_remove.push(file_name.clone());
                if let Err(e) = tokio::fs::remove_file(path).await {
                    eprintln!("清理缓存文件失败 {:?}: {}", path, e);
                }
            }
        }

        for file_name in to_remove {
            self.cached_files.remove(&file_name);
        }
    }
}

/// 内存流结构体，用于包装提取的数据
struct MemoryStream {
    data: std::io::Cursor<Vec<u8>>,
}

impl AsyncRead for MemoryStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::io::Read;
        match self.data.read(buf.initialize_unfilled()) {
            Ok(n) => {
                buf.advance(n);
                std::task::Poll::Ready(Ok(()))
            }
            Err(e) => std::task::Poll::Ready(Err(e)),
        }
    }
}

#[async_trait]
impl UpdatingSource for GiteeProtocol {
    async fn request(&mut self, path: &str, range: &Range<u64>, _desc: &str, config: &GlobalConfig) -> DownloadResult {
        eprintln!("GiteeProtocol请求: path={}, range={:?}-{:?}", path, range.start, range.end);
        
        // 对于gitee，我们总是完整下载文件，然后提取需要的部分
        // 这样可以避免gitee不支持Range请求的问题

        // 完整下载文件
        let file_path = match self.download_and_cache_file(path, config).await {
            Ok(path) => path,
            Err(err) => {
                eprintln!("下载文件失败: {:?}", err);
                return Ok(Err(err));
            },
        };

        // 提取指定范围的数据
        let data = match self.extract_range_from_file(&file_path, range).await {
            Ok(data) => data,
            Err(err) => {
                eprintln!("提取数据失败: {:?}", err);
                return Ok(Err(err));
            },
        };

        let len = data.len() as u64;

        // 创建一个内存流来模拟AsyncRead
        let stream = MemoryStream {
            data: std::io::Cursor::new(data),
        };

        eprintln!("请求完成，返回数据长度: {}", len);
        Ok(Ok((len, Box::pin(stream))))
    }

    fn mask_keyword(&self) -> &str {
        &self.mask_keyword
    }
}