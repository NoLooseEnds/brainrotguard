"""Tests for web/cache.py request-row and active-row filtering."""

from types import SimpleNamespace

from web.cache import build_active_row, build_requests_row, get_profile_cache


def test_build_requests_row_includes_active_video_even_if_allowlist_only_has_name(video_store):
    video_store.add_channel("LEGO", "allowed")
    video_store.add_video(
        "lego1234567",
        "LEGO City Adventure",
        "LEGO",
        channel_id="UCP-Ng5SXUEt0VE-TXqRdL6g",
    )
    video_store.update_status("lego1234567", "approved")

    state = SimpleNamespace(video_store=video_store, word_filter_cache=None)

    assert [video["video_id"] for video in build_requests_row(state)] == ["lego1234567"]


def test_build_active_row_includes_allowlisted_channel_videos(video_store):
    """Active row shows all approved videos, including those from allowlisted channels."""
    video_store.add_channel("LEGO", "allowed")
    video_store.add_video(
        "lego1234567",
        "LEGO City Adventure",
        "LEGO",
        channel_id="UCP-Ng5SXUEt0VE-TXqRdL6g",
    )
    video_store.update_status("lego1234567", "approved")

    state = SimpleNamespace(video_store=video_store, word_filter_cache=None)

    active = build_active_row(state)
    assert any(v["video_id"] == "lego1234567" for v in active)


def test_build_active_row_matches_channel_id_filter_against_cached_channel_name(video_store):
    video_store.add_channel("LEGO", "allowed", channel_id="UCP-Ng5SXUEt0VE-TXqRdL6g")
    video_store.add_video(
        "legoactive1",
        "LEGO Active Build",
        "LEGO",
        duration=180,
    )
    video_store.update_status("legoactive1", "approved")

    state = SimpleNamespace(
        video_store=video_store,
        word_filter_cache=None,
        channel_caches={},
        catalog_caches={},
        catalog_cache_times={},
    )
    cache = get_profile_cache(state, "default")
    cache["id_to_name"]["UCP-Ng5SXUEt0VE-TXqRdL6g"] = "LEGO"

    active = build_active_row(
        state,
        profile_id="default",
        channel_filter="UCP-Ng5SXUEt0VE-TXqRdL6g",
    )

    assert [video["video_id"] for video in active] == ["legoactive1"]
