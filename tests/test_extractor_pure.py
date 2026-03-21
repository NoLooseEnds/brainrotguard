"""Tests for pure (no-network) functions in youtube/extractor.py."""

import pytest

from youtube.extractor import (
    extract_video_id,
    format_duration,
    _safe_thumbnail,
    _is_short_url,
    _build_playback_payload,
    THUMB_ALLOWED_HOSTS,
    normalize_audio_language,
    parse_audio_language_priority,
)


class TestExtractVideoId:
    @pytest.mark.parametrize("input_val, expected", [
        # Standard URLs
        ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
        ("http://youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
        ("https://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
        # Shorts URL
        ("https://www.youtube.com/shorts/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
        # Plain video ID
        ("dQw4w9WgXcQ", "dQw4w9WgXcQ"),
        # With extra params
        ("https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=120", "dQw4w9WgXcQ"),
        # With whitespace
        ("  dQw4w9WgXcQ  ", "dQw4w9WgXcQ"),
    ])
    def test_valid_extraction(self, input_val, expected):
        assert extract_video_id(input_val) == expected

    @pytest.mark.parametrize("input_val", [
        "",
        "not a video!",
        "https://example.com/watch?v=abc",
        "12345",  # Too short
        "dQw4w9WgXcQ_extra",  # Too long
    ])
    def test_invalid_returns_none(self, input_val):
        assert extract_video_id(input_val) is None


class TestFormatDuration:
    @pytest.mark.parametrize("seconds, expected", [
        (0, "?"),
        (None, "?"),
        (60, "1:00"),
        (323, "5:23"),
        (3735, "1:02:15"),
        (5, "0:05"),
        (3600, "1:00:00"),
        (7261, "2:01:01"),
    ])
    def test_format(self, seconds, expected):
        assert format_duration(seconds) == expected


class TestSafeThumbnail:
    def test_allowed_host_passes(self):
        url = "https://i.ytimg.com/vi/abc/hqdefault.jpg"
        assert _safe_thumbnail(url, "dQw4w9WgXcQ") == url

    def test_all_allowed_hosts(self):
        for host in THUMB_ALLOWED_HOSTS:
            url = f"https://{host}/vi/abc/default.jpg"
            assert _safe_thumbnail(url, "dQw4w9WgXcQ") == url

    def test_disallowed_host_falls_back(self):
        url = "https://evil.com/thumb.jpg"
        result = _safe_thumbnail(url, "dQw4w9WgXcQ")
        assert result == "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"

    def test_http_scheme_falls_back(self):
        url = "http://i.ytimg.com/vi/abc/default.jpg"
        result = _safe_thumbnail(url, "dQw4w9WgXcQ")
        assert result == "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"

    def test_none_url_falls_back(self):
        result = _safe_thumbnail(None, "dQw4w9WgXcQ")
        assert result == "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"

    def test_empty_url_falls_back(self):
        result = _safe_thumbnail("", "dQw4w9WgXcQ")
        assert result == "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"

    def test_invalid_video_id_returns_empty(self):
        assert _safe_thumbnail("", "") == ""
        assert _safe_thumbnail(None, "invalid!!!") == ""


class TestIsShortUrl:
    def test_shorts_url(self):
        assert _is_short_url("https://www.youtube.com/shorts/dQw4w9WgXcQ") is True

    def test_regular_url(self):
        assert _is_short_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ") is False

    def test_none(self):
        assert _is_short_url(None) is False

    def test_empty(self):
        assert _is_short_url("") is False


class TestAudioLanguagePriority:
    def test_normalize_audio_language_aliases(self):
        assert normalize_audio_language("nb_NO") == "no"
        assert normalize_audio_language("Norwegian") == "no"
        assert normalize_audio_language("sv-SE") == "sv"
        assert normalize_audio_language("english") == "en"

    def test_parse_audio_language_priority_deduplicates_and_defaults(self):
        assert parse_audio_language_priority("nb, no, sv, english") == ["no", "sv", "en"]
        assert parse_audio_language_priority("") == ["no", "sv", "en"]


class TestBuildPlaybackPayload:
    def test_build_playback_payload_selects_preferred_language(self):
        info = {
            "title": "Dubbed Video",
            "duration": 120,
            "formats": [
                {
                    "format_id": "22",
                    "protocol": "https",
                    "url": "https://rr1---sn-test.googlevideo.com/videoplayback?id=22",
                    "acodec": "mp4a.40.2",
                    "vcodec": "avc1.4d401f",
                    "ext": "mp4",
                    "language": "en",
                    "height": 720,
                    "width": 1280,
                    "format_note": "English",
                },
                {
                    "format_id": "23",
                    "protocol": "https",
                    "url": "https://rr1---sn-test.googlevideo.com/videoplayback?id=23",
                    "acodec": "mp4a.40.2",
                    "vcodec": "avc1.4d401f",
                    "ext": "mp4",
                    "language": "no",
                    "height": 720,
                    "width": 1280,
                    "format_note": "Norwegian",
                },
            ],
        }

        payload = _build_playback_payload("abc12345678", info, "no, en")

        assert payload is not None
        assert payload["selected_language"] == "no"
        assert payload["selected_stream_id"] == "23:no"
        assert [option["code"] for option in payload["language_options"]] == ["no", "en"]

    def test_build_playback_payload_prefers_hls_multi_audio_when_available(self):
        info = {
            "title": "Adaptive Dubbed Video",
            "duration": 240,
            "formats": [
                {
                    "format_id": "233-hls-0",
                    "protocol": "m3u8_native",
                    "url": "https://manifest.googlevideo.com/api/manifest/hls_audio_no.m3u8",
                    "acodec": "mp4a.40.2",
                    "vcodec": "none",
                    "ext": "mp4",
                    "language": "nb-NO",
                    "format_note": "Norwegian",
                },
                {
                    "format_id": "233-hls-1",
                    "protocol": "m3u8_native",
                    "url": "https://manifest.googlevideo.com/api/manifest/hls_audio_en.m3u8",
                    "acodec": "mp4a.40.2",
                    "vcodec": "none",
                    "ext": "mp4",
                    "language": "en-US",
                    "format_note": "English",
                },
                {
                    "format_id": "234-hls-1",
                    "protocol": "m3u8_native",
                    "url": "https://manifest.googlevideo.com/api/manifest/hls_audio_en_high.m3u8",
                    "acodec": "mp4a.40.2",
                    "vcodec": "none",
                    "ext": "mp4",
                    "language": "en-US",
                    "format_note": "English",
                    "tbr": 192,
                },
                {
                    "format_id": "231-hls",
                    "protocol": "m3u8_native",
                    "url": "https://manifest.googlevideo.com/api/manifest/hls_720.m3u8",
                    "acodec": "none",
                    "vcodec": "avc1.64001F",
                    "ext": "mp4",
                    "width": 1280,
                    "height": 720,
                    "tbr": 2400,
                },
            ],
        }

        payload = _build_playback_payload("abc12345678", info, "no, en")

        assert payload is not None
        assert payload["mode"] == "hls"
        assert payload["master_manifest_url"] == "https://manifest.googlevideo.com/api/manifest/hls_audio_no.m3u8"
        assert payload["selected_language"] == "no"
        assert payload["selected_stream_id"] == "233-hls-0:no"
        assert {track["language"] for track in payload["audio_tracks"]} == {"no", "en"}
        assert any(track["stream_id"] == "234-hls-1:en" for track in payload["audio_tracks"])
        assert payload["video_variants"][0]["format_id"] == "231-hls"

    def test_build_playback_payload_falls_back_to_english_when_priority_missing(self):
        info = {
            "title": "Dubbed Video",
            "duration": 120,
            "formats": [
                {
                    "format_id": "22",
                    "protocol": "https",
                    "url": "https://rr1---sn-test.googlevideo.com/videoplayback?id=22",
                    "acodec": "mp4a.40.2",
                    "vcodec": "avc1.4d401f",
                    "ext": "mp4",
                    "language": "sv",
                    "height": 720,
                    "width": 1280,
                    "format_note": "Swedish",
                },
                {
                    "format_id": "23",
                    "protocol": "https",
                    "url": "https://rr1---sn-test.googlevideo.com/videoplayback?id=23",
                    "acodec": "mp4a.40.2",
                    "vcodec": "avc1.4d401f",
                    "ext": "mp4",
                    "language": "en",
                    "height": 720,
                    "width": 1280,
                    "format_note": "English",
                },
            ],
        }

        payload = _build_playback_payload("abc12345678", info, "no")

        assert payload is not None
        assert payload["selected_language"] == "en"
        assert payload["selected_stream_id"] == "23:en"

    def test_build_playback_payload_filters_translated_auto_subtitles(self):
        info = {
            "title": "Captioned Video",
            "duration": 120,
            "formats": [
                {
                    "format_id": "22",
                    "protocol": "https",
                    "url": "https://rr1---sn-test.googlevideo.com/videoplayback?id=22",
                    "acodec": "mp4a.40.2",
                    "vcodec": "avc1.4d401f",
                    "ext": "mp4",
                    "language": "en",
                    "height": 720,
                    "width": 1280,
                },
            ],
            "subtitles": {
                "en": [
                    {
                        "ext": "vtt",
                        "url": "https://www.youtube.com/api/timedtext?v=abc12345678&lang=en&fmt=vtt",
                        "name": "English",
                    }
                ]
            },
            "automatic_captions": {
                "sv": [
                    {
                        "ext": "vtt",
                        "url": "https://www.youtube.com/api/timedtext?v=abc12345678&lang=en&tlang=sv&fmt=vtt",
                        "name": "Swedish",
                    }
                ],
                "en": [
                    {
                        "ext": "vtt",
                        "url": "https://www.youtube.com/api/timedtext?v=abc12345678&kind=asr&lang=en&fmt=vtt",
                        "name": "English",
                    }
                ],
            },
        }

        payload = _build_playback_payload("abc12345678", info, "en")

        assert payload is not None
        assert [option["code"] for option in payload["subtitle_options"]] == ["en"]
        assert payload["subtitle_options"][0]["auto"] is False
