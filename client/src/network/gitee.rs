use std::ops::Range;
use std::pin::Pin;

use async_trait::async_trait;
use reqwest::Client;
use reqwest::ClientBuilder;
use serde_json::Value;
use tokio::io::AsyncRead;
use tokio::time::Duration;

use crate::error::BusinessError;
use crate::global_config::GlobalConfig;
use crate::network::DownloadResult;
use crate::network::UpdatingSource;
use crate::network::http::AsyncStreamBody;

/// 代表gitee更新协议
pub struct GiteeProtocol {
    /// gitee api的url
    pub api_url: String,

    /// http客户端对象
    pub client: Client,

    /// 打码的关键字，所有日志里的这个关键字都会被打码
    mask_keyword: String,

    /// 当前这个更新协议的编号，用来做debug用途
    index: u32,
}

impl GiteeProtocol {
    pub fn new(api_url: &str, config: &GlobalConfig, index: u32) -> Self {
        let client = ClientBuilder::new()
            .connect_timeout(Duration::from_millis(config.http_timeout as u64))
            .read_timeout(Duration::from_millis(config.http_timeout as u64))
            .danger_accept_invalid_certs(config.http_ignore_certificate)
            .build()
            .unwrap();

        let mask_keyword = match reqwest::Url::parse(api_url) {
            Ok(parsed) => parsed.host_str().unwrap_or("").to_owned(),
            Err(_) => "".to_owned(),
        };

        Self {
            api_url: api_url.to_owned(),
            client,
            mask_keyword,
            index,
        }
    }

    /// 从gitee api响应中获取文件的实际下载URL
    async fn get_file_download_url(&self, file_name: &str, config: &GlobalConfig) -> Result<String, BusinessError> {
        // 发送请求获取文件列表
        let rsp = self.client.get(&self.api_url)
            .send()
            .await
            .map_err(|e| BusinessError::new(format!("请求gitee api失败: {:?}", e)))?;

        let code = rsp.status().as_u16();
        if code < 200 || code >= 300 {
            let body = rsp.text().await.unwrap_or_else(|_| "".to_string());
            return Err(BusinessError::new(format!("gitee api返回错误状态码 {}: {}", code, body)));
        }

        let json_text = rsp.text().await
            .map_err(|e| BusinessError::new(format!("读取gitee api响应失败: {:?}", e)))?;

        // 解析JSON响应
        let json_value: Value = serde_json::from_str(&json_text)
            .map_err(|e| BusinessError::new(format!("解析gitee api响应JSON失败: {:?}", e)))?;

        // 查找指定文件的download_url
        if let Some(files) = json_value.as_array() {
            for file in files {
                if let Some(name) = file["name"].as_str() {
                    if name == file_name {
                        if let Some(download_url) = file["download_url"].as_str() {
                            return Ok(download_url.to_string());
                        }
                    }
                }
            }
        }

        Err(BusinessError::new(format!("在gitee api响应中未找到文件: {}", file_name)))
    }
}

#[async_trait]
impl UpdatingSource for GiteeProtocol {
    async fn request(&mut self, path: &str, range: &Range<u64>, desc: &str, config: &GlobalConfig) -> DownloadResult {
        // 获取文件的实际下载URL
        let download_url = match self.get_file_download_url(path, config).await {
            Ok(url) => url,
            Err(err) => return Ok(Err(err)),
        };

        // 构建请求
        let mut req = self.client.get(&download_url);
        
        // 检查是否需要部分文件下载
        let partial_file = range.start > 0 || range.end > 0;
        if partial_file {
            assert!(range.end >= range.start);
            req = req.header("Range", format!("bytes={}-{}", range.start, range.end - 1));
        }
        
        let req = req.build().unwrap();

        // 发起请求
        let rsp = match self.client.execute(req).await {
            Ok(rsp) => rsp,
            Err(err) => return Err(std::io::Error::new(std::io::ErrorKind::Other, err)),
        };

        let code = rsp.status().as_u16();

        // 检查状态码
        if (!partial_file && (code < 200 || code >= 300)) || (partial_file && code != 206) {
            let mut body = rsp.text().await.map_or_else(|e| format!("{:?}", e), |v| v);
            body.truncate(300);
            return Ok(Err(BusinessError::new(format!("服务器({})返回了{}而不是206: {} ({})\n{}", self.index, code, path, desc, body))));
        }

        let len = match rsp.content_length() {
            Some(len) => len,
            None => return Ok(Err(BusinessError::new(format!("服务器({})没有返回content-length头: {} ({})", self.index, path, desc)))),
        };

        if (range.end - range.start) > 0 && len != range.end - range.start {
            return Ok(Err(BusinessError::new(format!("服务器({})返回的content-length头 {} 不等于{}: {}", self.index, len, range.end - range.start, path))));
        }

        Ok(Ok((len, Box::pin(AsyncStreamBody(rsp, None)))))
    }

    fn mask_keyword(&self) -> &str {
        &self.mask_keyword
    }
}