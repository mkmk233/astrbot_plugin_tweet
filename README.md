# astrbot_plugin_tweet

从 `nonebot_tweet` 迁移到 AstrBot 的插件版本。

功能：
- 自动识别 `x.com / twitter.com` 推文链接
- 从指定 RSSHub 实例拉取推文文本、图片、视频
- 自动识别推特动图（animated_gif），转换为 GIF 后转发；转换失败自动回退 MP4
- RSSHub 只提供静态封面时，自动用 fxtwitter 元数据补全动图/视频地址并去除重复静态封面
- 默认模式下使用 AstrBot 内置 LLM 接口自动翻译推文
- 支持 `c/content`（仅媒体）与 `o/origin`（仅原文）前缀
- 自动识别并拉取 `booth.pm` 商品名称与图片

## 触发方式
- 直接发送推文链接：`https://x.com/<user>/status/<id>`
- 仅媒体：`c https://x.com/<user>/status/<id>` 或 `content https://...`
- 仅原文：`o https://x.com/<user>/status/<id>` 或 `origin https://...`
- 发送 BOOTH 链接：`https://booth.pm/xx/items/<id>`

## 配置项
通过 `_conf_schema.json` 在 AstrBot 管理面板配置：
- `rsshub_base_url`: RSSHub 基础地址（默认 `https://rsshub.app/twitter/user/`）
- `rsshub_query_param`: 附加查询参数
- `translate_enabled`: 是否启用翻译
- `translate_target_language`: 翻译目标语言
- `translate_provider_id`: 翻译使用的 provider（留空则跟随当前会话）
- `translate_fallback_provider_ids`: 翻译失败时按顺序重试的 provider 列表
- `detect_language_before_translate`: 翻译前语言识别（优先 Google 免 Key，失败回退 LLM）
- `fxtwitter_media_fallback_enabled`: RSSHub 媒体信息不完整时，用 fxtwitter 补全真实动图/视频地址（默认开启）
- `animated_gif_enabled`: 是否把推特动图转换为 GIF 后转发
- `fxtwitter_gif_proxy_enabled`: 优先使用 fxtwitter GIF CDN 获取动图，避免部分网络中 `video.twimg.com` 直连失败
- `animated_gif_fps`: GIF 帧率（默认 12）
- `animated_gif_max_width`: GIF 最大宽度，0 表示保持原始分辨率（默认 480）
- `animated_gif_max_duration_sec`: GIF 最大时长，0 表示不限（默认 15）
- `animated_gif_convert_timeout_sec`: 单个 GIF 的转换超时秒数（默认 120）
- `booth_locale`: BOOTH 接口语言区域
- `request_timeout_sec`: 网络请求超时秒数

## 说明
- 翻译调用使用 AstrBot 的 `context.llm_generate()` 内置接口实现。
- 当 `translate_provider_id` 为空时，会自动使用当前会话的聊天模型。
- 当 `translate_fallback_provider_ids` 为空且 `translate_provider_id` 也为空时，会自动继承当前会话配置中的 `provider_settings.fallback_chat_models`。
- 当主翻译模型请求异常、返回错误响应，或返回空文本时，插件会按 fallback 顺序继续重试。
- 动图默认优先从 fxtwitter 的 GIF CDN 获取；如果不可用，再下载原 MP4 并使用 `imageio-ffmpeg` 内置 FFmpeg 本地转换。
- `video.twimg.com` 在部分网络环境无法直连时，fxtwitter GIF CDN 兜底可以继续保证动图以多帧 GIF 发送到 QQ。
- 发送前会检查 GIF 文件头和多帧属性；转换结果不是动画时不会冒充 GIF，而是回退到原 MP4。
- RSSHub 已提供真实动图地址时不会请求 fxtwitter 元数据接口；只有 RSSHub 缺少视频/动图地址且检出了视频封面时才触发元数据兜底。
- 动图转换在原文消息发送后于后台执行，不会阻塞推文文本、图片与翻译的转发。
