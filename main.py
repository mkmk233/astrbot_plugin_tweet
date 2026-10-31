from __future__ import annotations

import asyncio
import html
import io
import json
import re
import socket
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import aiohttp

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, MessageEventResult, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_tweet",
    "crazt",
    "基于 RSSHub 的推文转发插件，支持内置 LLM 翻译、RSSHub 媒体兜底、动图 GIF 转换与 BOOTH 商品信息抓取。",
    "1.2.0",
    "https://github.com/icrazt/astrbot_plugin_tweet",
)
class TweetPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._background_tasks: set[asyncio.Task] = set()
        self._cache_lock = asyncio.Lock()
        self._cache_path: Optional[Path] = None

        self.twitter_link_pattern = re.compile(
            r"https?://(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)",
            re.IGNORECASE,
        )
        self.status_link_pattern = re.compile(
            r"(?:x|twitter)\.com/([A-Za-z0-9_]+)/status/(\d+)",
            re.IGNORECASE,
        )
        self.booth_link_pattern = re.compile(
            r"https://(?:[A-Za-z0-9-]+\.)?booth\.pm/(?:[a-z\-]+/)?items/(\d+)",
            re.IGNORECASE,
        )
        self.image_url_pattern = re.compile(
            r"https://pbs\.twimg\.com/(?:media|tweet_video_thumb|amplify_video_thumb|ext_tw_video_thumb)/[^\"'<>\s]+",
            re.IGNORECASE,
        )
        self.video_url_pattern = re.compile(
            r"https://video\.twimg\.com/[^\"'<>\s]+",
            re.IGNORECASE,
        )

    @filter.event_message_type(filter.EventMessageType.ALL, priority=5)
    async def on_all_message(self, event: AstrMessageEvent):
        text = (event.message_str or "").strip()
        if not text:
            return

        tweet_info = await self._handle_tweet_link(event, text)
        if tweet_info is not None:
            tweet_results, translate_text = tweet_info
            event.should_call_llm(False)

            umo = event.unified_msg_origin
            translate_task: Optional[asyncio.Task] = None
            if translate_text:
                # 并发：立即启动翻译请求，同时发送原文
                translate_task = asyncio.create_task(
                    self._translate_text(umo=umo, text=translate_text)
                )

            # 以前"原文无文字"的推文直接 yield 给 respond 阶段发送，插件拿不到
            # 发送结果：NapCat 超时（retcode=1200）时消息根本进不了群，却没有任何
            # 兜底。这里和带文字的分支统一成"插件自发送 + 失败改发卡片图片"。
            chains = [
                list(getattr(result, "chain", []) or [])
                for result in tweet_results
            ]
            image_count = sum(
                isinstance(segment, Comp.Image)
                for parts in chains
                for segment in parts
            )
            # 三张及以上图片时将推文内容合并为一条消息，避免逐图刷屏。
            if image_count >= 3:
                chains = [[segment for parts in chains for segment in parts]]

            for parts in chains:
                chain = MessageChain(chain=parts)
                if not chain.chain:
                    continue
                try:
                    await self.context.send_message(umo, chain)
                except Exception as exc:
                    # 平台/客户端拒收或超时：改用渲染好的单张图片再试一次
                    logger.warning(
                        f"tweet plugin: 原文发送失败，尝试改发图片兜底: {exc}"
                    )
                    self._schedule_card_fallback(event, reason="send_failed")
                    break

            if translate_task is not None:
                # 原文已发送，等待翻译完成后发送
                try:
                    translated = await translate_task
                    if translated:
                        await self.context.send_message(
                            umo, MessageChain().message(translated)
                        )
                except Exception as exc:
                    logger.warning(f"tweet plugin: 翻译失败: {exc}")
            event.stop_event()
            return

        booth_results = await self._handle_booth_link(event, text)
        if booth_results is not None:
            event.should_call_llm(False)
            for result in booth_results:
                result.stop_event()
                yield result

    async def _handle_tweet_link(
        self,
        event: AstrMessageEvent,
        text: str,
    ) -> Optional[tuple[list[MessageEventResult], Optional[str]]]:
        command: Optional[str] = None
        content = text
        lowered = text.lower()

        if lowered.startswith("c ") or lowered.startswith("content "):
            command = "content"
            content = text.split(" ", 1)[1] if " " in text else ""
        elif lowered.startswith("o ") or lowered.startswith("origin "):
            command = "origin"
            content = text.split(" ", 1)[1] if " " in text else ""

        match = self.twitter_link_pattern.search(content)
        if not match:
            return None

        user_name = match.group(1)
        tweet_id = match.group(2)
        original_link = match.group(0)

        if user_name.lower() == "i":
            resolved = await self._resolve_twitter_link(tweet_id)
            if not resolved:
                return ([event.plain_result("未能解析推文链接。")], None)
            user_name, original_link = resolved

        # 1 天内处理过的推文直接复用缓存（原文 + 译文），跳过 RSSHub 和翻译
        if command is None and self._cfg_bool("tweet_cache_enabled", True):
            cached = await self._cache_lookup(tweet_id)
            if cached is not None:
                chain = self._build_tweet_original(cached, user_name)
                if chain:
                    logger.info(f"tweet plugin: 命中缓存 tweet_id={tweet_id}")
                    return ([event.chain_result(chain)], cached.get("translated") or None)

        rsshub_base_url = self._cfg_str(
            "rsshub_base_url",
            "https://rsshub.app/twitter/user/",
        )
        if not rsshub_base_url:
            return ([event.plain_result("插件尚未配置 RSSHub 地址，请联系管理员。")], None)

        query = self._cfg_str("rsshub_query_param", "")
        if query and not query.startswith("?"):
            query = f"?{query}"
        rss_url = f"{rsshub_base_url.rstrip('/')}/{user_name}/status/{tweet_id}{query}"

        event.set_extra(
            "astrbot_plugin_tweet_ref",
            {"user": user_name, "id": tweet_id, "link": original_link},
        )

        tweet_data = await self._fetch_tweet_data(rss_url, original_link)
        if not tweet_data:
            # 原文解析失败：把推文渲染成图片兜底发送
            event.set_extra("astrbot_plugin_tweet_card_only", True)
            chain = await self._build_card_fallback_chain(
                user_name=user_name,
                tweet_id=tweet_id,
                reason="feed_unavailable",
                link=original_link,
                fallback_text="未能获取该推文。",
            )
            return ([event.chain_result(chain)], None)

        if command == "content":
            chain = self._build_tweet_content_only(tweet_data)
        else:
            chain = self._build_tweet_original(tweet_data, user_name)

        gif_urls = [
            u for u in tweet_data.get("gifs", [])
            if self._is_valid_twitter_video_url(u)
        ]
        video_urls = [
            u for u in tweet_data.get("videos", [])
            if self._is_valid_twitter_video_url(u)
        ]
        if not self._cfg_bool("animated_gif_enabled", True):
            # 关闭转换时完全沿用原有视频发送链路。
            video_urls = [*gif_urls, *video_urls]
            gif_urls = []
        if gif_urls:
            task = asyncio.create_task(
                self._send_gifs_followup(
                    umo=event.unified_msg_origin,
                    gif_urls=gif_urls,
                )
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        if video_urls:
            task = asyncio.create_task(
                self._send_videos_followup(
                    umo=event.unified_msg_origin,
                    video_urls=video_urls,
                )
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

        if not chain:
            if gif_urls and video_urls:
                chain = [Comp.Plain("检测到动图和视频，正在发送媒体消息...")]
            elif gif_urls:
                chain = [Comp.Plain("检测到动图，正在转换为 GIF 并发送...")]
            elif video_urls:
                chain = [Comp.Plain("检测到视频，正在发送视频消息...")]
            else:
                event.set_extra("astrbot_plugin_tweet_card_only", True)
                chain = await self._build_card_fallback_chain(
                    user_name=user_name,
                    tweet_id=tweet_id,
                    reason="empty_content",
                    link=original_link,
                    fallback_text="该推文没有可发送的文本、图片或视频。",
                )

        translate_text = None
        if command is None:
            source_text = str(tweet_data.get("text") or "").strip()
            if source_text:
                translate_text = source_text
            if self._cfg_bool("tweet_cache_enabled", True):
                # 后台翻译完成后落缓存，不阻塞原文发送
                task = asyncio.create_task(
                    self._translate_and_cache(user_name, tweet_id, tweet_data, source_text)
                )
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

        return ([event.chain_result(chain)], translate_text)

    # ---------- 推文缓存（1 天内相同推文直接复用原文 + 译文） ----------

    def _cache_file(self) -> Path:
        if self._cache_path is None:
            try:
                from astrbot.core.utils.astrbot_path import get_astrbot_data_path

                base = Path(get_astrbot_data_path()) / "plugin_data" / "astrbot_plugin_tweet"
            except Exception:
                base = Path("/AstrBot/data/plugin_data/astrbot_plugin_tweet")
            base.mkdir(parents=True, exist_ok=True)
            self._cache_path = base / "tweet_cache.json"
        return self._cache_path

    def _cache_ttl_sec(self) -> int:
        hours = self._cfg_int("tweet_cache_ttl_hours", 24)
        return max(1, hours) * 3600

    async def _cache_lookup(self, tweet_id: str) -> Optional[dict[str, Any]]:
        """返回未过期的缓存条目；顺手清理过期条目。"""
        async with self._cache_lock:
            path = self._cache_file()
            try:
                data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            except Exception:
                data = {}
            now = time.time()
            ttl = self._cache_ttl_sec()
            fresh = {k: v for k, v in data.items() if now - float(v.get("ts", 0)) < ttl}
            if len(fresh) != len(data):
                try:
                    path.write_text(
                        json.dumps(fresh, ensure_ascii=False), encoding="utf-8"
                    )
                except Exception as exc:
                    logger.warning(f"tweet plugin: 清理缓存失败: {exc}")
            entry = fresh.get(str(tweet_id))
        if not entry:
            return None
        return {
            "text": entry.get("text") or "",
            "images": entry.get("images") or [],
            "pub_date": entry.get("pub_date") or "",
            "author": entry.get("author") or "",
            "translated": entry.get("translated") or "",
        }

    async def _cache_store(
        self,
        user_name: str,
        tweet_id: str,
        tweet_data: dict[str, Any],
        translated: str,
    ) -> None:
        async with self._cache_lock:
            path = self._cache_file()
            try:
                data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            except Exception:
                data = {}
            now = time.time()
            ttl = self._cache_ttl_sec()
            data = {k: v for k, v in data.items() if now - float(v.get("ts", 0)) < ttl}
            data[str(tweet_id)] = {
                "user": user_name,
                "text": str(tweet_data.get("text") or ""),
                "images": list(tweet_data.get("images") or []),
                "pub_date": str(tweet_data.get("pub_date") or ""),
                "author": str(tweet_data.get("author") or ""),
                "translated": translated,
                "ts": now,
            }
            try:
                path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            except Exception as exc:
                logger.warning(f"tweet plugin: 写入缓存失败: {exc}")

    async def _translate_and_cache(
        self,
        user_name: str,
        tweet_id: str,
        tweet_data: dict[str, Any],
        source_text: str,
    ) -> None:
        """后台任务：把原文和译文写入缓存，供 1 天内的重复推文直接复用。"""
        translated = ""
        if source_text:
            # 主流程几乎同时发起翻译；稍等让它先占用 provider，再取自己的结果落库
            await asyncio.sleep(2)
            translated = await self._translate_text(umo="", text=source_text) or ""
        await self._cache_store(user_name, tweet_id, tweet_data, translated)

    async def _handle_booth_link(
        self,
        event: AstrMessageEvent,
        text: str,
    ) -> Optional[list[MessageEventResult]]:
        match = self.booth_link_pattern.search(text)
        if not match:
            return None

        item_id = match.group(1)
        booth_data = await self._fetch_booth_data(item_id)
        if not booth_data:
            return [event.plain_result("未能获取该 BOOTH 商品信息。")]

        name = str(booth_data.get("name") or "").strip()
        images = self._extract_booth_images(booth_data)

        if not name and not images:
            return [event.plain_result("该 BOOTH 商品没有可发送的信息。")]

        booth_chain = []
        if name:
            booth_chain.append(Comp.Plain(f"{name}\n"))
        for image_url in images:
            try:
                booth_chain.append(Comp.Image.fromURL(image_url))
            except Exception as exc:
                logger.warning(f"tweet plugin: BOOTH 图片构建失败 {image_url} - {exc}")

        if not booth_chain and name:
            return [event.plain_result(name)]
        if not booth_chain:
            return [event.plain_result("该 BOOTH 商品没有可发送的信息。")]

        platform_name = (event.get_platform_name() or "").lower()
        is_onebot_like = ("aiocqhttp" in platform_name) or ("onebot" in platform_name)
        is_group_message = bool(event.get_group_id())

        if len(images) >= 5 and is_group_message and is_onebot_like:
            try:
                node = Comp.Node(
                    uin=event.get_self_id() or "0",
                    name="BOOTH",
                    content=booth_chain,
                )
                return [event.chain_result([node])]
            except Exception as exc:
                logger.warning(f"tweet plugin: BOOTH 合并转发构建失败，降级普通发送: {exc}")

        return [event.chain_result(booth_chain)]

    async def _fetch_tweet_data(
        self,
        rss_url: str,
        original_link: str,
    ) -> Optional[dict[str, Any]]:
        logger.debug(f"tweet plugin: fetching RSS data from {rss_url}")
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {"User-Agent": "astrbot-plugin-tweet/1.2.0"}

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(rss_url) as response:
                    if response.status >= 400:
                        logger.warning(f"tweet plugin: RSS HTTP {response.status} for {rss_url}")
                        return None
                    xml_text = await response.text()
        except Exception as exc:
            logger.warning(f"tweet plugin: failed to fetch RSS feed: {exc}")
            return None

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning(f"tweet plugin: failed to parse RSS XML: {exc}")
            return None

        items = root.findall(".//item")
        if not items:
            return None

        match = self.status_link_pattern.search(original_link)
        if not match:
            return None
        original_user, original_tweet_id = match.groups()

        for item in reversed(items):
            guid = (item.findtext("guid") or "").strip()
            link = (item.findtext("link") or "").strip()
            link_blob = f"{guid} {link}"

            guid_match = self.status_link_pattern.search(link_blob)
            if not guid_match:
                continue

            guid_user, guid_tweet_id = guid_match.groups()
            if (
                guid_user.lower() != original_user.lower()
                or guid_tweet_id != original_tweet_id
            ):
                continue

            raw_content = item.findtext("description") or ""
            text, image_urls, gif_urls, video_urls = (
                self._extract_text_images_videos(raw_content)
            )
            tweet_data = {
                "text": text,
                "images": image_urls,
                "gifs": gif_urls,
                "videos": video_urls,
                "pub_date": item.findtext("pubDate") or "",
                "author": item.findtext("author") or "",
            }
            return await self._enrich_tweet_media_from_fxtwitter(
                tweet_data,
                original_user,
                original_tweet_id,
            )
        return None

    async def _enrich_tweet_media_from_fxtwitter(
        self,
        tweet_data: dict[str, Any],
        user_name: str,
        tweet_id: str,
    ) -> dict[str, Any]:
        """Supplement incomplete RSSHub media metadata.

        Some RSSHub instances only include the static ``tweet_video_thumb`` for a
        Twitter animated GIF.  In that case the RSS item contains no
        ``video.twimg.com/tweet_video`` URL and the old implementation had no way
        to know that the media was animated.  Querying fxtwitter here keeps the
        RSSHub path intact while recovering the real animated media URL.
        """
        if not self._cfg_bool("fxtwitter_media_fallback_enabled", True):
            return tweet_data
        if tweet_data.get("gifs") or tweet_data.get("videos"):
            # RSSHub 常同时给出真实视频地址和静态封面。移除封面，避免
            # 已经发送动画后又重复发送一张看似静态的图片。
            tweet_data["images"] = [
                image_url
                for image_url in tweet_data.get("images") or []
                if not self._is_video_thumbnail_url(image_url)
            ]
            return tweet_data
        image_urls = list(tweet_data.get("images") or [])
        if not any(self._is_video_thumbnail_url(url) for url in image_urls):
            return tweet_data

        api_url = f"https://api.fxtwitter.com/{user_name}/status/{tweet_id}"
        timeout = aiohttp.ClientTimeout(
            total=self._cfg_int("request_timeout_sec", 20),
        )
        headers = {
            "User-Agent": "astrbot-plugin-tweet/1.2.0",
            # fxtwitter may negotiate Brotli; avoid requiring an optional
            # brotli decoder in the host AstrBot environment.
            "Accept-Encoding": "identity",
        }
        try:
            async with aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
            ) as session:
                async with session.get(api_url) as response:
                    if response.status >= 400:
                        logger.debug(
                            "tweet plugin: fxtwitter media fallback HTTP %s for %s",
                            response.status,
                            api_url,
                        )
                        return tweet_data
                    payload = await response.json(content_type=None)
        except Exception as exc:
            logger.debug(
                "tweet plugin: fxtwitter media fallback failed for %s - %s",
                api_url,
                exc,
            )
            return tweet_data

        tweet = payload.get("tweet") if isinstance(payload, dict) else None
        media = tweet.get("media") if isinstance(tweet, dict) else None
        media_items = media.get("all") if isinstance(media, dict) else None
        if not isinstance(media_items, list):
            return tweet_data

        gif_urls = list(tweet_data.get("gifs") or [])
        video_urls = list(tweet_data.get("videos") or [])
        media_refs: list[tuple[str, str]] = []

        for media_item in media_items:
            if not isinstance(media_item, dict):
                continue
            media_url = str(media_item.get("url") or "").strip()
            media_type = str(media_item.get("type") or "").strip().lower()
            thumbnail_url = str(media_item.get("thumbnail_url") or "").strip()
            if not self._is_valid_twitter_video_url(media_url):
                continue
            if media_type == "gif":
                gif_urls.append(media_url)
            elif media_type == "video":
                video_urls.append(media_url)
            else:
                continue
            media_refs.append((media_url, thumbnail_url))

        if not media_refs:
            return tweet_data

        filtered_images = [
            image_url
            for image_url in image_urls
            if not any(
                self._is_related_video_thumbnail(
                    image_url,
                    media_url,
                    thumbnail_url,
                )
                for media_url, thumbnail_url in media_refs
            )
        ]
        tweet_data["images"] = self._dedup_urls(filtered_images)
        tweet_data["gifs"] = self._dedup_urls(gif_urls)
        tweet_data["videos"] = self._dedup_urls(video_urls)
        logger.info(
            "tweet plugin: fxtwitter media fallback recovered %d GIF(s), %d video(s) for %s",
            len(tweet_data["gifs"]),
            len(tweet_data["videos"]),
            api_url,
        )
        return tweet_data

    def _is_video_thumbnail_url(self, image_url: str) -> bool:
        image_path = urlparse(image_url).path.lower()
        return any(
            marker in image_path
            for marker in (
                "/tweet_video_thumb/",
                "/amplify_video_thumb/",
                "/ext_tw_video_thumb/",
            )
        )

    def _shrink_twimg_image_url(self, url: str) -> str:
        """把 pbs.twimg.com 的原图降到 large 档，避免发送超时。

        ``?name=orig`` 的原图动辄 4-8 MB（实测 2894x4093 = 4.28 MB），而 AstrBot
        的 aiocqhttp 适配器还会把它整张转成 base64（体积 +33%）再推给 NapCat，
        QQ 客户端超时就回 retcode=1200 Timeout，整条消息发不出去。同一张图
        large 档约 323 KB，内容一致。
        """
        if not isinstance(url, str) or "pbs.twimg.com" not in url:
            return url
        if self._is_video_thumbnail_url(url):
            # 视频/GIF 的静态封面只在视频发不出去时用到，保持原样
            return url
        if re.search(r"[?&]name=orig\b", url):
            return re.sub(r"([?&]name=)orig\b", r"\1large", url)
        if re.search(r"[?&]name=", url):
            # 已经是别的档位，不重复改
            return url
        return url + ("&" if "?" in url else "?") + "name=large"

    def _is_related_video_thumbnail(
        self,
        image_url: str,
        media_url: str,
        thumbnail_url: str = "",
    ) -> bool:
        if thumbnail_url and image_url.split("?", 1)[0] == thumbnail_url.split("?", 1)[0]:
            return True

        if not self._is_video_thumbnail_url(image_url):
            return False

        image_path = urlparse(image_url).path.lower()
        image_stem = Path(image_path).stem
        media_stem = Path(urlparse(media_url).path.lower()).stem
        return bool(image_stem and image_stem == media_stem)

    async def _resolve_twitter_link(self, tweet_id: str) -> Optional[tuple[str, str]]:
        api_url = f"https://api.vxtwitter.com/i/status/{tweet_id}"
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {"User-Agent": "astrbot-plugin-tweet/1.2.0"}

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(api_url) as response:
                    if response.status >= 400:
                        logger.warning(
                            f"tweet plugin: failed to resolve i/status link HTTP {response.status}",
                        )
                        return None
                    data = await response.json(content_type=None)
        except Exception as exc:
            logger.warning(f"tweet plugin: failed to resolve tweet link: {exc}")
            return None

        tweet_url = data.get("tweetURL")
        screen_name = data.get("user_screen_name")
        if not tweet_url or not screen_name:
            return None
        return str(screen_name), str(tweet_url)

    async def _fetch_booth_data(self, item_id: str) -> Optional[dict[str, Any]]:
        booth_locale = self._cfg_str("booth_locale", "zh-cn")
        api_url = f"https://booth.pm/{booth_locale}/items/{item_id}.json"
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {"User-Agent": "astrbot-plugin-tweet/1.2.0"}

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(api_url) as response:
                    if response.status >= 400:
                        logger.warning(
                            f"tweet plugin: BOOTH API HTTP {response.status} for item {item_id}",
                        )
                        return None
                    data = await response.json(content_type=None)
        except Exception as exc:
            logger.warning(f"tweet plugin: failed to fetch BOOTH item: {exc}")
            return None
        if isinstance(data, dict):
            return data
        return None

    async def _send_videos_followup(self, umo: str, video_urls: list[str]) -> None:
        for video_url in video_urls:
            await self._send_video_followup_item(umo, video_url)
            await asyncio.sleep(0.8)

    async def _send_video_followup_item(self, umo: str, video_url: str) -> None:
        try:
            ok = await self.context.send_message(
                umo,
                MessageChain(chain=[Comp.Video.fromURL(video_url)]),
            )
            if not ok:
                logger.warning(f"tweet plugin: 无法发送视频消息，找不到会话: {umo}")
        except Exception as exc:
            logger.warning(f"tweet plugin: 发送视频消息失败 {video_url} - {exc}")

    async def _send_gifs_followup(self, umo: str, gif_urls: list[str]) -> None:
        for gif_url in gif_urls:
            await self._send_gif_followup_item(umo, gif_url)
            await asyncio.sleep(0.8)

    async def _send_gif_followup_item(self, umo: str, video_url: str) -> None:
        if not self._cfg_bool("animated_gif_enabled", True):
            await self._send_video_followup_item(umo, video_url)
            return

        with tempfile.TemporaryDirectory(prefix="astrbot_tweet_gif_") as temp_dir:
            source_path = Path(temp_dir) / "source.mp4"
            gif_path = Path(temp_dir) / "animated.gif"

            converted = False
            if self._cfg_bool("fxtwitter_gif_proxy_enabled", True):
                converted = await self._download_fxtwitter_gif(video_url, gif_path)
                if converted:
                    logger.info(
                        "tweet plugin: 已通过 fxtwitter GIF CDN 获取动图，跳过 MP4 直连转换",
                    )

            if not converted:
                downloaded = await self._download_twitter_video(video_url, source_path)
                converted = downloaded and await self._convert_video_file_to_gif(
                    source_path,
                    gif_path,
                )
            if not converted:
                logger.warning(
                    f"tweet plugin: 动图获取/转换失败，回退发送原 MP4: {video_url}",
                )
                await self._send_video_followup_item(umo, video_url)
                return

            sent = False
            try:
                sent = await self.context.send_message(
                    umo,
                    MessageChain(chain=[Comp.Image.fromFileSystem(gif_path)]),
                )
                if not sent:
                    logger.warning(f"tweet plugin: 无法发送 GIF 消息，找不到会话: {umo}")
            except Exception as exc:
                logger.warning(f"tweet plugin: 发送 GIF 消息失败 {video_url} - {exc}")

            if not sent:
                logger.warning(f"tweet plugin: GIF 消息未成功发送，回退发送原 MP4: {video_url}")
                await self._send_video_followup_item(umo, video_url)

    async def _download_twitter_video(
        self,
        video_url: str,
        output_path: Path,
    ) -> bool:
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {"User-Agent": "astrbot-plugin-tweet/1.2.0"}

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(video_url) as response:
                    if response.status >= 400:
                        logger.warning(
                            f"tweet plugin: 下载动图 MP4 HTTP {response.status}: {video_url}",
                        )
                        return False
                    with output_path.open("wb") as output_file:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            output_file.write(chunk)
        except Exception as exc:
            logger.warning(f"tweet plugin: 下载动图 MP4 失败 {video_url} - {exc}")
            return False

        if output_path.stat().st_size == 0:
            logger.warning(f"tweet plugin: 下载的动图 MP4 为空: {video_url}")
            return False
        return True

    def _build_fxtwitter_gif_url(self, video_url: str) -> str:
        """将 Twitter animated_gif 的 MP4 地址映射到 fxtwitter GIF CDN。"""
        parsed = urlparse(video_url)
        if (
            parsed.scheme != "https"
            or parsed.netloc.lower() != "video.twimg.com"
            or not parsed.path.lower().startswith("/tweet_video/")
        ):
            return ""

        gif_path = re.sub(
            r"\.(?:mp4|m4v)$",
            ".gif",
            parsed.path,
            flags=re.IGNORECASE,
        )
        if not gif_path.lower().endswith(".gif"):
            return ""
        return f"https://gif.fxtwitter.com{gif_path}"

    async def _download_fxtwitter_gif(
        self,
        video_url: str,
        output_path: Path,
    ) -> bool:
        """通过 fxtwitter CDN 获取已转换 GIF，绕过部分网络下的 twimg 不可达。"""
        gif_url = self._build_fxtwitter_gif_url(video_url)
        if not gif_url:
            return False

        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {
            "User-Agent": "astrbot-plugin-tweet/1.2.0",
            "Accept-Encoding": "identity",
        }
        try:
            # 该 CDN 同时公布 IPv4/IPv6；部分国内网络下 IPv6 会先超时，
            # 强制 IPv4 可显著缩短首包等待并避免误判 CDN 不可用。
            connector = aiohttp.TCPConnector(family=socket.AF_INET)
            async with aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                connector=connector,
            ) as session:
                async with session.get(gif_url) as response:
                    if response.status >= 400:
                        logger.debug(
                            "tweet plugin: fxtwitter GIF HTTP %s: %s",
                            response.status,
                            gif_url,
                        )
                        return False
                    with output_path.open("wb") as output_file:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            output_file.write(chunk)
        except Exception as exc:
            logger.debug(
                "tweet plugin: fxtwitter GIF 下载失败 %s - %s",
                gif_url,
                exc,
            )
            return False

        if output_path.stat().st_size == 0:
            logger.debug(f"tweet plugin: fxtwitter GIF 为空: {gif_url}")
            return False
        if not self._is_animated_gif_file(output_path):
            logger.warning(f"tweet plugin: fxtwitter 返回的文件不是有效动画 GIF: {gif_url}")
            return False
        logger.info(
            "tweet plugin: fxtwitter GIF 下载完成 size=%d url=%s",
            output_path.stat().st_size,
            gif_url,
        )
        return True

    async def _convert_video_file_to_gif(
        self,
        source_path: Path,
        output_path: Path,
    ) -> bool:
        try:
            import imageio_ffmpeg

            ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception as exc:
            logger.warning(
                f"tweet plugin: 无法加载内置 FFmpeg，请重新安装插件依赖: {exc}",
            )
            return False

        fps = max(1, min(self._cfg_int("animated_gif_fps", 12), 30))
        max_width = max(0, self._cfg_int("animated_gif_max_width", 480))
        max_duration = max(0, self._cfg_int("animated_gif_max_duration_sec", 15))

        video_filters = [f"fps={fps}"]
        if max_width:
            video_filters.append(
                f"scale=w='min({max_width},iw)':h=-2:flags=lanczos",
            )
        video_filters.append(
            "split[s0][s1];[s0]palettegen=stats_mode=diff[p];"
            "[s1][p]paletteuse=dither=sierra2_4a:diff_mode=rectangle",
        )

        command = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(source_path),
        ]
        if max_duration:
            command.extend(["-t", str(max_duration)])
        command.extend(
            [
                "-vf",
                ",".join(video_filters),
                "-an",
                "-loop",
                "0",
                str(output_path),
            ]
        )

        timeout_sec = max(
            self._cfg_int("animated_gif_convert_timeout_sec", 120),
            30,
        )
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass
            logger.warning(f"tweet plugin: GIF 转换超时 ({timeout_sec}s): {source_path.name}")
            return False
        except Exception as exc:
            logger.warning(f"tweet plugin: 启动 GIF 转换失败: {exc}")
            return False

        if process.returncode != 0:
            error_text = stderr.decode(errors="replace").strip()
            logger.warning(
                f"tweet plugin: FFmpeg GIF 转换失败 ({process.returncode}): {error_text}",
            )
            return False
        if not output_path.exists() or output_path.stat().st_size == 0:
            logger.warning("tweet plugin: FFmpeg 未生成有效的 GIF 文件")
            return False
        if not self._is_animated_gif_file(output_path):
            logger.warning("tweet plugin: FFmpeg 输出不是有效的多帧 GIF，拒绝按 GIF 发送")
            return False
        return True

    def _is_animated_gif_file(self, gif_path: Path) -> bool:
        try:
            with gif_path.open("rb") as gif_file:
                signature = gif_file.read(6)
        except OSError as exc:
            logger.warning(f"tweet plugin: 无法读取待发送 GIF: {exc}")
            return False

        if signature not in (b"GIF87a", b"GIF89a"):
            logger.warning("tweet plugin: GIF 文件头无效")
            return False

        # AstrBot normally ships Pillow; if it is unavailable still allow a
        # file with a valid GIF signature because FFmpeg already produced it.
        try:
            from PIL import Image
        except ImportError:
            return True

        try:
            with Image.open(gif_path) as image:
                frames = int(getattr(image, "n_frames", 1))
                is_animated = bool(getattr(image, "is_animated", False))
                if image.format != "GIF" or frames <= 1 or not is_animated:
                    logger.warning(
                        "tweet plugin: GIF 校验失败 format=%s frames=%d animated=%s",
                        image.format,
                        frames,
                        is_animated,
                    )
                    return False
                logger.info(
                    "tweet plugin: 已生成可动画 GIF frames=%d size=%s",
                    frames,
                    image.size,
                )
                return True
        except Exception as exc:
            logger.warning(f"tweet plugin: 无法校验 GIF 动画属性: {exc}")
            return False

    async def _translate_text(
        self,
        umo: str,
        text: Optional[str],
    ) -> Optional[str]:
        if not text:
            return None
        if not self._cfg_bool("translate_enabled", True):
            return None

        target_language = self._cfg_str("translate_target_language", "zh-Hans")
        if not target_language:
            return None

        provider_ids = await self._resolve_translation_provider_ids(umo=umo)
        if not provider_ids:
            return None

        raw_text = text.strip()
        if not raw_text:
            return None

        if self._cfg_bool("detect_language_before_translate", False):
            detected_language = await self._detect_language(provider_ids, raw_text)
            if detected_language and self._language_matches_target(
                detected_language,
                target_language,
            ):
                return None

        translated_text = await self._request_translation(
            provider_ids=provider_ids,
            target_language=target_language,
            text=raw_text,
            system_prompt="你是翻译助手。只输出翻译结果，不要解释。",
        )

        if not translated_text or translated_text == raw_text:
            translated_text = await self._request_translation(
                provider_ids=provider_ids,
                target_language=target_language,
                text=raw_text,
                system_prompt="Translate the given text and output translation only.",
            )

        if not translated_text:
            return None

        return f"{translated_text}"

    async def _request_translation(
        self,
        provider_ids: list[str],
        target_language: str,
        text: str,
        system_prompt: str,
    ) -> Optional[str]:
        prompt = (
            f"请将以下文本翻译为 {target_language}。\n"
            "仅输出翻译结果，不要解释：\n\n"
            f"{text}"
        )
        return await self._generate_with_fallback(
            provider_ids=provider_ids,
            system_prompt=system_prompt,
            prompt=prompt,
            purpose="translation",
        )

    async def _detect_language(
        self,
        provider_ids: list[str],
        text: str,
    ) -> Optional[str]:
        detected_by_google = await self._detect_language_by_google(text)
        if detected_by_google:
            logger.info(
                f"tweet plugin: language detection API=GoogleFree detected={detected_by_google}",
            )
            return detected_by_google

        logger.info("tweet plugin: language detection GoogleFree unavailable, fallback to LLM")
        detected_by_llm = await self._detect_language_by_llm(provider_ids, text)
        if detected_by_llm:
            logger.info(
                f"tweet plugin: language detection API=LLM providers={provider_ids} detected={detected_by_llm}",
            )
        return detected_by_llm

    async def _detect_language_by_google(self, text: str) -> Optional[str]:
        sample_text = text.strip()
        if not sample_text:
            return None

        # 避免 URL 过长导致检测接口失败，截取前段原文用于判定语言
        if len(sample_text) > 2000:
            sample_text = sample_text[:2000]

        api_url = "https://translate.googleapis.com/translate_a/single"
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {"User-Agent": "astrbot-plugin-tweet/1.2.0"}
        params = {
            "client": "gtx",
            "sl": "auto",
            "tl": "en",
            "dt": "t",
            "q": sample_text,
        }

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(api_url, params=params) as response:
                    if response.status >= 400:
                        logger.warning(
                            f"tweet plugin: Google language detect HTTP {response.status}",
                        )
                        return None
                    data = await response.json(content_type=None)
        except Exception as exc:
            logger.warning(f"tweet plugin: Google language detect failed: {exc}")
            return None

        detected_language = None
        if isinstance(data, list) and len(data) >= 3 and isinstance(data[2], str):
            detected_language = data[2].strip()

        if not detected_language:
            logger.warning("tweet plugin: Google language detect returned unexpected payload")
            return None

        code_match = re.search(
            r"\b([A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*)\b",
            detected_language,
        )
        if not code_match:
            logger.warning("tweet plugin: Google language detect returned invalid language code")
            return None
        return code_match.group(1)

    async def _detect_language_by_llm(
        self,
        provider_ids: list[str],
        text: str,
    ) -> Optional[str]:
        response_text = await self._generate_with_fallback(
            provider_ids=provider_ids,
            system_prompt="你是语言识别助手。只返回语言代码，如 en、zh-Hans、ja。",
            prompt=text,
            purpose="language detection",
        )
        if not response_text:
            return None

        code_match = re.search(
            r"\b([A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*)\b",
            response_text,
        )
        if not code_match:
            logger.warning("tweet plugin: fallback LLM language detection returned invalid language code")
            return None
        return code_match.group(1)

    async def _resolve_translation_provider_ids(self, umo: str) -> list[str]:
        provider_ids: list[str] = []
        provider_id = self._cfg_str("translate_provider_id", "")
        using_current_chat_provider = False
        fallback_source = "plugin_config"

        if not provider_id:
            using_current_chat_provider = True
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo=umo)
            except Exception as exc:
                logger.warning(
                    f"tweet plugin: failed to resolve chat provider for translation: {exc}",
                )
                return []

        if provider_id:
            provider_ids.append(provider_id)

        fallback_ids = self._cfg_list("translate_fallback_provider_ids")
        if not fallback_ids and using_current_chat_provider:
            fallback_ids = self._get_session_fallback_provider_ids(umo)
            fallback_source = "session_fallback_chat_models"
        elif not fallback_ids:
            fallback_source = "none"

        for fallback_id in fallback_ids:
            if fallback_id and fallback_id not in provider_ids:
                provider_ids.append(fallback_id)

        logger.info(
            "tweet plugin: translation providers resolved primary=%s fallback_source=%s fallback_count=%d providers=%s",
            provider_id,
            fallback_source,
            max(len(provider_ids) - 1, 0),
            provider_ids,
        )
        return provider_ids

    def _get_session_fallback_provider_ids(self, umo: str) -> list[str]:
        try:
            provider_manager = getattr(self.context, "provider_manager", None)
            acm = getattr(provider_manager, "acm", None)
            if not acm:
                return []
            conf = acm.get_conf(umo)
            if not isinstance(conf, dict):
                return []
            provider_settings = conf.get("provider_settings", {})
            if not isinstance(provider_settings, dict):
                return []
            fallback_ids = provider_settings.get("fallback_chat_models", [])
            if not isinstance(fallback_ids, list):
                return []
            return [
                str(fallback_id).strip()
                for fallback_id in fallback_ids
                if str(fallback_id).strip()
            ]
        except Exception as exc:
            logger.warning(
                f"tweet plugin: failed to resolve session fallback chat models: {exc}",
            )
            return []

    async def _generate_with_fallback(
        self,
        *,
        provider_ids: list[str],
        system_prompt: str,
        prompt: str,
        purpose: str,
    ) -> Optional[str]:
        total = len(provider_ids)
        logger.info(
            "tweet plugin: starting %s request with %d provider(s): %s",
            purpose,
            total,
            provider_ids,
        )
        for index, provider_id in enumerate(provider_ids):
            logger.info(
                "tweet plugin: trying %s provider %s (%d/%d)",
                purpose,
                provider_id,
                index + 1,
                total,
            )
            try:
                llm_resp = await self.context.llm_generate(
                    chat_provider_id=provider_id,
                    system_prompt=system_prompt,
                    prompt=prompt,
                )
            except Exception as exc:
                logger.warning(
                    f"tweet plugin: {purpose} request failed with provider {provider_id} ({index + 1}/{total}): {exc}",
                )
                continue

            if llm_resp.role == "err":
                logger.warning(
                    "tweet plugin: %s provider %s (%d/%d) returned error response%s",
                    purpose,
                    provider_id,
                    index + 1,
                    total,
                    ", trying fallback" if index < total - 1 else "",
                )
                continue

            content = (llm_resp.completion_text or "").strip()
            if content:
                logger.info(
                    "tweet plugin: %s succeeded with provider %s (%d/%d)",
                    purpose,
                    provider_id,
                    index + 1,
                    total,
                )
                return content

            logger.warning(
                "tweet plugin: %s provider %s (%d/%d) returned empty response%s",
                purpose,
                provider_id,
                index + 1,
                total,
                ", trying fallback" if index < total - 1 else "",
            )
        logger.warning(
            "tweet plugin: all providers failed for %s: %s",
            purpose,
            provider_ids,
        )
        return None

    # ---------------- 推文转图片兜底 ----------------

    def _sanitize_card_text(self, text: str) -> str:
        """去掉 Noto Sans CJK 无法渲染的 emoji 码位，避免图片里出现豆腐块。"""
        kept = []
        for char in text or "":
            code = ord(char)
            if code > 0xFFFF or code == 0x200D or 0xFE00 <= code <= 0xFE0F:
                continue
            # ✨✅❌ 等 BMP 区的符号/装饰字符同样不在 CJK 字体里
            if 0x2600 <= code <= 0x27BF or 0x2B00 <= code <= 0x2BFF:
                continue
            kept.append(char)
        return "".join(kept)

    def _card_font_path(self) -> Optional[Path]:
        plugin_dir = Path(__file__).resolve().parent
        candidates = [
            plugin_dir / "fonts" / "NotoSansCJKsc-Regular.otf",
            plugin_dir / "fonts" / "NotoSansCJKsc-Regular.ttf",
            Path(
                "/AstrBot/data/plugin_data/astrbot_plugin_shoubanhua/fonts/"
                "NotoSansCJKsc-Regular.otf"
            ),
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        ]
        for candidate in candidates:
            try:
                if candidate.exists():
                    return candidate
            except OSError:
                continue
        return None

    def _card_output_dir(self) -> Path:
        try:
            from astrbot.api.star import StarTools

            base = Path(StarTools.get_data_dir("astrbot_plugin_tweet"))
        except Exception:
            base = (
                Path(__file__).resolve().parents[2]
                / "plugin_data"
                / "astrbot_plugin_tweet"
            )
        base.mkdir(parents=True, exist_ok=True)
        return base

    def _prune_card_files(self, keep_days: int = 7) -> None:
        try:
            cutoff = time.time() - keep_days * 86400
            for path in self._card_output_dir().glob("card_*.png"):
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink()
                except OSError:
                    continue
        except Exception:
            return

    async def _fetch_fxtwitter_tweet(
        self, user_name: str, tweet_id: str
    ) -> Optional[dict[str, Any]]:
        """直接向 fxtwitter 取推文元数据（不依赖 RSSHub / LLM）。"""
        api_url = f"https://api.fxtwitter.com/{user_name}/status/{tweet_id}"
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {
            "User-Agent": "astrbot-plugin-tweet/1.2.0",
            "Accept-Encoding": "identity",
        }
        try:
            async with aiohttp.ClientSession(
                timeout=timeout, headers=headers
            ) as session:
                async with session.get(api_url) as response:
                    if response.status >= 400:
                        logger.warning(
                            f"tweet plugin: 图片兜底取推文元数据 HTTP {response.status}: {api_url}"
                        )
                        return None
                    payload = await response.json(content_type=None)
        except Exception as exc:
            logger.warning(f"tweet plugin: 图片兜底取推文元数据失败 {api_url} - {exc}")
            return None

        if not isinstance(payload, dict) or payload.get("code") != 200:
            logger.warning(f"tweet plugin: 图片兜底拿到异常响应: {api_url}")
            return None
        tweet = payload.get("tweet")
        return tweet if isinstance(tweet, dict) else None

    async def _download_card_media(self, url: str) -> Optional[bytes]:
        if not url:
            return None
        timeout = aiohttp.ClientTimeout(total=self._cfg_int("request_timeout_sec", 20))
        headers = {
            "User-Agent": "astrbot-plugin-tweet/1.2.0",
            "Accept-Encoding": "identity",
        }
        try:
            async with aiohttp.ClientSession(
                timeout=timeout, headers=headers
            ) as session:
                async with session.get(url) as response:
                    if response.status >= 400:
                        logger.warning(
                            f"tweet plugin: 图片兜底下载素材 HTTP {response.status}: {url}"
                        )
                        return None
                    return await response.read()
        except Exception as exc:
            logger.warning(f"tweet plugin: 图片兜底下载素材失败 {url} - {exc}")
            return None

    def _format_card_date(self, raw: str) -> str:
        raw = (raw or "").strip()
        parsed: Optional[datetime] = None
        for fmt in ("%a %b %d %H:%M:%S %z %Y", "%a, %d %b %Y %H:%M:%S %z"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if not parsed:
            return ""
        return parsed.astimezone(timezone(timedelta(hours=8))).strftime("%y-%m-%d %H:%M")

    def _wrap_card_text(
        self,
        draw,
        font,
        text: str,
        max_width: int,
        max_lines: int = 34,
    ) -> list[str]:
        lines: list[str] = []
        for paragraph in (text or "").split("\n"):
            if not paragraph.strip():
                lines.append("")
                continue
            current = ""
            for char in paragraph:
                candidate = current + char
                if not current or draw.textlength(candidate, font=font) <= max_width:
                    current = candidate
                else:
                    lines.append(current)
                    current = char
            lines.append(current)
        while lines and not lines[-1].strip():
            lines.pop()
        if len(lines) > max_lines:
            lines = lines[:max_lines]
            if lines:
                lines[-1] = lines[-1].rstrip() + "…"
        return lines

    def _fit_card_image(self, image, box_w: int, box_h: int):
        from PIL import Image

        ratio = min(box_w / max(image.width, 1), box_h / max(image.height, 1))
        size = (
            max(1, int(image.width * ratio)),
            max(1, int(image.height * ratio)),
        )
        return image.resize(size, Image.LANCZOS)

    def _compose_tweet_card(
        self,
        output_path: Path,
        display_name: str,
        screen_name: str,
        text: str,
        created_at: str,
        avatar_bytes: Optional[bytes],
        media_payloads: list,
        font_path: Path,
        tweet: dict[str, Any],
    ) -> None:
        from PIL import Image, ImageDraw, ImageFont, ImageOps

        width = max(480, min(self._cfg_int("fallback_card_width", 720), 1200))
        pad = 28
        gap = 12
        content_width = width - pad * 2

        name_font = ImageFont.truetype(str(font_path), 30)
        handle_font = ImageFont.truetype(str(font_path), 21)
        body_font = ImageFont.truetype(str(font_path), 26)
        meta_font = ImageFont.truetype(str(font_path), 20)

        bg = (21, 32, 43)
        cell_bg = (30, 41, 54)
        text_color = (231, 233, 234)
        name_color = (255, 255, 255)
        dim_color = (136, 153, 166)
        line_color = (47, 51, 54)

        images = []
        for payload, media_type in media_payloads:
            try:
                with Image.open(io.BytesIO(payload)) as handle:
                    images.append((handle.convert("RGB").copy(), media_type))
            except Exception as exc:
                logger.warning(f"tweet plugin: 图片兜底素材解码失败 - {exc}")

        image_count = len(images)
        columns = 0 if image_count == 0 else (1 if image_count == 1 else 2)
        rows = (image_count + columns - 1) // columns if columns else 0
        cell_w = (content_width - gap * (columns - 1)) // columns if columns else 0
        if columns == 1:
            cell_h = min(int(cell_w * 1.33), 560)
        else:
            cell_h = min(cell_w, 340)

        measure = ImageDraw.Draw(Image.new("RGB", (8, 8)))
        header_text = f"@{screen_name}" + (f" · {created_at}" if created_at else "")
        while header_text and measure.textlength(header_text, font=handle_font) > (
            content_width - 100
        ):
            header_text = header_text[:-2]
        footer_metrics = []
        for label, key in (("赞", "likes"), ("转推", "retweets"), ("浏览", "views")):
            value = tweet.get(key)
            if isinstance(value, int) and value > 0:
                footer_metrics.append(f"{label} {value:,}")
        footer_text = " · ".join(footer_metrics)

        body_lines: list[str] = []
        if text:
            body_lines = self._wrap_card_text(measure, body_font, text, content_width)
        line_height = 40

        header_h = 68
        cursor = pad
        body_y = cursor + header_h + (20 if body_lines else 0)
        if body_lines:
            cursor = body_y + line_height * len(body_lines)
        else:
            cursor = cursor + header_h
        media_y = cursor + (22 if rows else 0)
        if rows:
            cursor = media_y + rows * cell_h + (rows - 1) * gap
        footer_y = cursor + (24 if footer_text else 0)
        height = footer_y + (26 + pad if footer_text else pad)

        card = Image.new("RGB", (width, height), bg)
        draw = ImageDraw.Draw(card)

        if avatar_bytes:
            try:
                with Image.open(io.BytesIO(avatar_bytes)) as handle:
                    avatar = ImageOps.fit(
                        handle.convert("RGB"), (header_h, header_h), Image.LANCZOS
                    )
                mask = Image.new("L", (header_h, header_h), 0)
                ImageDraw.Draw(mask).ellipse((0, 0, header_h - 1, header_h - 1), fill=255)
                card.paste(avatar, (pad, pad), mask)
            except Exception as exc:
                logger.warning(f"tweet plugin: 图片兜底头像解码失败 - {exc}")
        else:
            draw.ellipse(
                (pad, pad, pad + header_h - 1, pad + header_h - 1),
                fill=cell_bg,
            )

        name_x = pad + header_h + 18
        draw.text(
            (name_x, pad + 4),
            display_name,
            font=name_font,
            fill=name_color,
            stroke_width=1,
            stroke_fill=name_color,
        )
        draw.text(
            (name_x, pad + 42),
            header_text,
            font=handle_font,
            fill=dim_color,
        )

        for index, line in enumerate(body_lines):
            draw.text(
                (pad, body_y + index * line_height),
                line,
                font=body_font,
                fill=text_color,
            )

        if rows:
            for index, (image, _media_type) in enumerate(images):
                row = index // columns
                column = index % columns
                cell_x = pad + column * (cell_w + gap)
                cell_y = media_y + row * (cell_h + gap)
                draw.rectangle(
                    (cell_x, cell_y, cell_x + cell_w, cell_y + cell_h),
                    fill=cell_bg,
                )
                fitted = self._fit_card_image(image, cell_w, cell_h)
                card.paste(
                    fitted,
                    (
                        cell_x + (cell_w - fitted.width) // 2,
                        cell_y + (cell_h - fitted.height) // 2,
                    ),
                )

        if footer_text:
            draw.line((pad, footer_y, width - pad, footer_y), fill=line_color, width=1)
            draw.text(
                (pad, footer_y + 12),
                footer_text,
                font=meta_font,
                fill=dim_color,
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        card.save(output_path, "PNG", optimize=True)

    async def _render_card_from_ref(
        self, user_name: str, tweet_id: str
    ) -> Optional[Path]:
        if not user_name or not tweet_id:
            return None
        tweet = await self._fetch_fxtwitter_tweet(user_name, tweet_id)
        if not tweet:
            return None

        try:
            from PIL import Image  # noqa: F401  仅用于提前确认依赖可用
        except ImportError as exc:
            logger.warning(f"tweet plugin: 图片兜底需要 Pillow: {exc}")
            return None

        font_path = self._card_font_path()
        if not font_path:
            logger.warning("tweet plugin: 图片兜底找不到可用字体")
            return None

        author = tweet.get("author") if isinstance(tweet.get("author"), dict) else {}
        screen_name = str(author.get("screen_name") or user_name).strip() or user_name
        display_name = str(author.get("name") or screen_name).strip() or screen_name
        text = self._sanitize_card_text(str(tweet.get("text") or "").strip())
        created_at = self._format_card_date(str(tweet.get("created_at") or ""))

        media_items: list[tuple[str, str]] = []
        media = tweet.get("media") if isinstance(tweet.get("media"), dict) else {}
        for item in (media.get("all") or [])[:4]:
            if not isinstance(item, dict):
                continue
            media_type = str(item.get("type") or "").strip().lower()
            if media_type == "photo":
                url = str(item.get("url") or "").strip()
            else:
                url = str(item.get("thumbnail_url") or "").strip()
            if url:
                media_items.append((url, media_type))

        avatar_bytes = await self._download_card_media(
            str(author.get("avatar_url") or "")
        )
        media_payloads = []
        for url, media_type in media_items:
            payload = await self._download_card_media(url)
            if payload:
                media_payloads.append((payload, media_type))

        card_path = self._card_output_dir() / (
            f"card_{tweet.get('id') or tweet_id}.png"
        )
        await asyncio.to_thread(
            self._compose_tweet_card,
            card_path,
            display_name,
            screen_name,
            text,
            created_at,
            avatar_bytes,
            media_payloads,
            font_path,
            tweet,
        )
        self._prune_card_files()
        logger.info(
            "tweet plugin: 已生成推文兜底图片 %s (%d bytes)",
            card_path,
            card_path.stat().st_size,
        )
        return card_path

    def _card_caption(self, reason: str, link: str) -> str:
        prefix = {
            "feed_unavailable": "推文解析失败，已改成图片发送",
            "empty_content": "推文没有可直接转发的媒体，已改成图片发送",
            "send_failed": "原文发送超时，已改发图片",
        }.get(reason, "推文已改成图片发送")
        return f"{prefix}\n{link}\n" if link else f"{prefix}\n"

    async def _build_card_fallback_chain(
        self,
        *,
        user_name: str,
        tweet_id: str,
        reason: str,
        link: str,
        fallback_text: str,
    ) -> list:
        if not self._cfg_bool("fallback_image_enabled", True):
            return [Comp.Plain(fallback_text)]
        try:
            card_path = await self._render_card_from_ref(user_name, tweet_id)
        except Exception as exc:
            logger.warning(f"tweet plugin: 图片兜底渲染异常 - {exc}")
            card_path = None
        if not card_path:
            return [Comp.Plain(fallback_text)]
        chain = [Comp.Plain(self._card_caption(reason, link))]
        try:
            chain.append(Comp.Image.fromFileSystem(card_path))
        except Exception as exc:
            logger.warning(f"tweet plugin: 图片兜底组件构建失败 - {exc}")
            return [Comp.Plain(fallback_text)]
        return chain

    async def _send_card_fallback(
        self,
        *,
        umo: str,
        user_name: str,
        tweet_id: str,
        reason: str,
        link: str,
    ) -> None:
        try:
            await asyncio.sleep(2)
            card_path = await self._render_card_from_ref(user_name, tweet_id)
            if not card_path:
                logger.warning(f"tweet plugin: 图片兜底渲染失败，放弃重发 {tweet_id}")
                return
            chain = [
                Comp.Plain(self._card_caption(reason, link)),
                Comp.Image.fromFileSystem(card_path),
            ]
            sent = await self.context.send_message(
                umo, MessageChain(chain=chain)
            )
            if sent:
                logger.info(f"tweet plugin: 图片兜底已发送 tweet={tweet_id}")
            else:
                logger.warning(f"tweet plugin: 图片兜底找不到会话 {umo}")
        except Exception as exc:
            logger.warning(f"tweet plugin: 图片兜底发送失败 - {exc}")

    def _schedule_card_fallback(self, event: AstrMessageEvent, *, reason: str) -> None:
        """发送失败后在后台渲染一张推文卡片补发。"""
        if event.get_extra("astrbot_plugin_tweet_card_only"):
            # 这一条本身就是兜底图片，再失败就放弃，避免无限兜底
            logger.warning("tweet plugin: 兜底图片本身发送失败，不再重复兜底")
            return
        ref = event.get_extra("astrbot_plugin_tweet_ref") or {}
        task = asyncio.create_task(
            self._send_card_fallback(
                umo=event.unified_msg_origin,
                user_name=str(ref.get("user") or ""),
                tweet_id=str(ref.get("id") or ""),
                reason=reason,
                link=str(ref.get("link") or ""),
            )
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _build_tweet_original(
        self,
        tweet_data: dict[str, Any],
        user_name: str,
    ) -> list:
        chain = []
        formatted_date = self._format_pub_date(tweet_data.get("pub_date", ""))
        author = str(tweet_data.get("author") or "").strip()
        if formatted_date and author:
            chain.append(Comp.Plain(f"{author}@{user_name} {formatted_date}\n"))

        text = str(tweet_data.get("text") or "").strip()
        if text:
            chain.append(Comp.Plain(f"{text}\n"))

        for image_url in tweet_data.get("images", []):
            try:
                chain.append(
                    Comp.Image.fromURL(self._shrink_twimg_image_url(image_url))
                )
            except Exception as exc:
                logger.warning(f"tweet plugin: failed to build image component {image_url} - {exc}")
        return chain

    def _build_tweet_content_only(self, tweet_data: dict[str, Any]) -> list:
        chain = []
        for image_url in tweet_data.get("images", []):
            try:
                chain.append(
                    Comp.Image.fromURL(self._shrink_twimg_image_url(image_url))
                )
            except Exception as exc:
                logger.warning(f"tweet plugin: failed to build image component {image_url} - {exc}")
        return chain

    def _extract_text_images_videos(
        self,
        content: str,
    ) -> tuple[str, list[str], list[str], list[str]]:
        content = html.unescape(content or "")

        image_urls = [
            u for u in self._dedup_urls(self.image_url_pattern.findall(content))
            if self._is_valid_twitter_media_url(u)
        ]
        media_video_urls = [
            u for u in self._dedup_urls(self.video_url_pattern.findall(content))
            if self._is_valid_twitter_video_url(u)
        ]
        gif_urls = [
            u for u in media_video_urls
            if self._is_twitter_animated_gif_url(u)
        ]
        video_urls = [
            u for u in media_video_urls
            if not self._is_twitter_animated_gif_url(u)
        ]

        cleaned = self.image_url_pattern.sub(" ", content)
        cleaned = self.video_url_pattern.sub(" ", cleaned)
        cleaned = re.sub(r"<video[^>]*>.*?</video>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<[^>]+>", "\n", cleaned)

        lines = []
        for line in cleaned.splitlines():
            line = re.sub(r"\s+", " ", line).strip()
            if line:
                lines.append(line)
        text = "\n".join(lines)

        return text, image_urls, gif_urls, video_urls

    def _extract_booth_images(self, booth_data: dict[str, Any]) -> list[str]:
        images = booth_data.get("images", [])
        if not isinstance(images, list):
            return []
        urls: list[str] = []
        for image in images:
            if not isinstance(image, dict):
                continue
            original_url = image.get("original")
            if isinstance(original_url, str) and original_url.strip():
                urls.append(original_url.strip())
        return self._dedup_urls(urls)

    def _format_pub_date(self, pub_date: str) -> str:
        if not pub_date:
            return ""
        parsed: Optional[datetime] = None
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
            try:
                parsed = datetime.strptime(pub_date, fmt)
                break
            except ValueError:
                continue
        if not parsed:
            return ""
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        east_asia = timezone(timedelta(hours=8))
        return parsed.astimezone(east_asia).strftime("%y-%m-%d %H:%M")

    def _language_matches_target(self, detected: str, target: str) -> bool:
        detected_norm = detected.strip().lower().replace("_", "-")
        target_norm = target.strip().lower().replace("_", "-")
        if not detected_norm or not target_norm:
            return False
        if detected_norm == target_norm:
            return True
        if detected_norm.startswith(target_norm) or target_norm.startswith(detected_norm):
            return True
        if detected_norm.startswith("zh") and target_norm.startswith("zh"):
            return True
        return False

    def _is_valid_twitter_media_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.scheme not in {"http", "https"}:
            return False
        if parsed.netloc.lower() != "pbs.twimg.com":
            return False
        file_name = parsed.path.rsplit("/", 1)[-1]
        # 截断 URL（如 .../media/HEA）会导致 onebot rich media 上传失败
        if len(file_name) < 6:
            return False
        # 大多数可用媒体链接会带 format 参数或显式扩展名
        if "format=" not in parsed.query and "." not in file_name:
            return False
        return True

    def _is_valid_twitter_video_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.scheme not in {"http", "https"}:
            return False
        return parsed.netloc.lower() == "video.twimg.com" and bool(parsed.path.strip("/"))

    def _is_twitter_animated_gif_url(self, url: str) -> bool:
        if not self._is_valid_twitter_video_url(url):
            return False
        # Twitter/X 会把上传的 GIF 转码为 video.twimg.com/tweet_video/*.mp4。
        return "/tweet_video/" in urlparse(url).path.lower()

    def _dedup_urls(self, urls: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for url in urls:
            normalized = url.strip().rstrip(".,;!?\"'" )
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _chunked(self, data: list[str], chunk_size: int) -> list[list[str]]:
        if chunk_size <= 0:
            return [data]
        return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

    def _cfg_str(self, key: str, default: str = "") -> str:
        value = self.config.get(key, default)
        if value is None:
            return default
        return str(value).strip()

    def _cfg_int(self, key: str, default: int) -> int:
        value = self.config.get(key, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _cfg_bool(self, key: str, default: bool) -> bool:
        value = self.config.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return bool(value)

    def _cfg_list(self, key: str) -> list[str]:
        value = self.config.get(key, [])
        if isinstance(value, str):
            items = re.split(r"[\n,]", value)
        elif isinstance(value, (list, tuple, set)):
            items = list(value)
        else:
            return []
        result: list[str] = []
        for item in items:
            normalized = str(item).strip()
            if normalized:
                result.append(normalized)
        return result
