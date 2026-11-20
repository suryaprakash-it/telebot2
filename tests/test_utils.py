"""Unit tests for pure utility functions — no I/O, no database, no network."""
from __future__ import annotations

import pytest
from app import (
    compute_etag,
    content_disposition,
    human_size,
    media_info,
    parse_byte_range,
    safe_name,
)


# ═══════════════════════════════════════════════════════════════════════════════
# parse_byte_range
# ═══════════════════════════════════════════════════════════════════════════════
class TestParseByteRange:
    """Full coverage of RFC 7233 single-range parsing."""

    def test_no_range_returns_full(self):
        start, end, status = parse_byte_range(None, 1000)
        assert (start, end, status) == (0, 999, 200)

    def test_simple_range(self):
        start, end, status = parse_byte_range("bytes=0-499", 1000)
        assert (start, end, status) == (0, 499, 206)

    def test_open_ended_range(self):
        start, end, status = parse_byte_range("bytes=500-", 1000)
        assert (start, end, status) == (500, 999, 206)

    def test_suffix_range(self):
        start, end, status = parse_byte_range("bytes=-200", 1000)
        assert (start, end, status) == (800, 999, 206)

    def test_suffix_larger_than_file(self):
        start, end, status = parse_byte_range("bytes=-5000", 1000)
        assert (start, end, status) == (0, 999, 206)

    def test_range_clamped_to_end(self):
        start, end, status = parse_byte_range("bytes=0-5000", 1000)
        assert (start, end, status) == (0, 999, 206)

    def test_exact_last_byte(self):
        start, end, status = parse_byte_range("bytes=999-999", 1000)
        assert (start, end, status) == (999, 999, 206)

    def test_first_byte_only(self):
        start, end, status = parse_byte_range("bytes=0-0", 1000)
        assert (start, end, status) == (0, 0, 206)

    def test_middle_range(self):
        start, end, status = parse_byte_range("bytes=100-200", 1000)
        assert (start, end, status) == (100, 200, 206)

    def test_start_beyond_size_raises(self):
        with pytest.raises(ValueError, match="outside"):
            parse_byte_range("bytes=1000-", 1000)

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError, match="outside"):
            parse_byte_range("bytes=500-100", 1000)

    def test_multipart_unsupported(self):
        with pytest.raises(ValueError, match="Unsupported"):
            parse_byte_range("bytes=0-100,200-300", 1000)

    def test_bare_dash_invalid(self):
        with pytest.raises(ValueError):
            parse_byte_range("bytes=-", 1000)

    def test_zero_suffix_invalid(self):
        with pytest.raises(ValueError):
            parse_byte_range("bytes=-0", 1000)

    def test_non_bytes_unit_invalid(self):
        with pytest.raises(ValueError):
            parse_byte_range("chars=0-100", 1000)

    def test_empty_string_returns_full(self):
        # Empty string is falsy → treated as "no range"
        start, end, status = parse_byte_range("", 1000)
        assert status == 200

    # --- 1 GB+ file simulation ---
    def test_large_file_range(self):
        size = 2 * 1024**3  # 2 GB
        start, end, status = parse_byte_range(f"bytes={size - 1024}-", size)
        assert start == size - 1024
        assert end == size - 1
        assert status == 206

    def test_large_suffix_range(self):
        size = 4 * 1024**3  # 4 GB
        start, end, status = parse_byte_range("bytes=-1048576", size)
        assert start == size - 1048576
        assert end == size - 1


# ═══════════════════════════════════════════════════════════════════════════════
# human_size
# ═══════════════════════════════════════════════════════════════════════════════
class TestHumanSize:
    def test_none(self):
        assert human_size(None) == "Size unavailable"

    def test_zero(self):
        assert human_size(0) == "0 B"

    def test_small_bytes(self):
        assert human_size(500) == "500 B"

    def test_one_kb(self):
        assert human_size(1024) == "1.0 KB"

    def test_kilobytes(self):
        result = human_size(1500)
        assert "KB" in result

    def test_megabytes(self):
        result = human_size(5 * 1024**2)
        assert "MB" in result

    def test_gigabytes(self):
        result = human_size(2 * 1024**3)
        assert "GB" in result

    def test_terabytes(self):
        result = human_size(3 * 1024**4)
        assert "TB" in result

    def test_exact_boundary(self):
        # 1 MB exactly
        assert "1.0 MB" == human_size(1024**2)

    def test_4gb(self):
        result = human_size(4 * 1024**3)
        assert "4.0 GB" == result


# ═══════════════════════════════════════════════════════════════════════════════
# safe_name
# ═══════════════════════════════════════════════════════════════════════════════
class TestSafeName:
    def test_normal(self):
        assert safe_name("report.pdf") == "report.pdf"

    def test_none(self):
        assert safe_name(None) == "telegram-file"

    def test_empty(self):
        assert safe_name("") == "telegram-file"

    def test_path_traversal(self):
        result = safe_name("../../etc/passwd")
        assert ".." not in result
        assert result == "passwd"

    def test_control_chars(self):
        result = safe_name("bad\x00\x1fname.txt")
        assert "\x00" not in result
        assert "\x1f" not in result

    def test_backslashes(self):
        assert safe_name("C:\\Users\\file.txt") == "file.txt"

    def test_forward_slashes(self):
        assert safe_name("path/to/file.txt") == "file.txt"

    def test_unicode(self):
        assert safe_name("документ.pdf") == "документ.pdf"

    def test_whitespace_only(self):
        assert safe_name("   ") == "telegram-file"


# ═══════════════════════════════════════════════════════════════════════════════
# content_disposition
# ═══════════════════════════════════════════════════════════════════════════════
class TestContentDisposition:
    def test_ascii(self):
        result = content_disposition("test.txt")
        assert 'filename="test.txt"' in result
        assert "filename*=UTF-8''" in result

    def test_unicode(self):
        result = content_disposition("файл.txt")
        assert "filename*=UTF-8''" in result
        # ASCII fallback must be pure ASCII
        fallback = result.split(";")[1].strip()
        assert fallback.startswith('filename="')

    def test_special_chars(self):
        result = content_disposition('file "name".txt')
        assert "\\\\" not in result.split("filename*")[0]

    def test_empty_becomes_default(self):
        result = content_disposition("")
        assert "telegram-file" in result


# ═══════════════════════════════════════════════════════════════════════════════
# compute_etag
# ═══════════════════════════════════════════════════════════════════════════════
class TestComputeEtag:
    def test_deterministic(self):
        r = {"token": "abc", "size_bytes": 1000}
        assert compute_etag(r) == compute_etag(r)

    def test_different_tokens(self):
        r1 = {"token": "abc", "size_bytes": 1000}
        r2 = {"token": "xyz", "size_bytes": 1000}
        assert compute_etag(r1) != compute_etag(r2)

    def test_different_sizes(self):
        r1 = {"token": "abc", "size_bytes": 1000}
        r2 = {"token": "abc", "size_bytes": 2000}
        assert compute_etag(r1) != compute_etag(r2)

    def test_format_quoted_hex(self):
        r = {"token": "abc", "size_bytes": 1000}
        etag = compute_etag(r)
        assert etag.startswith('"') and etag.endswith('"')
        assert len(etag) == 18  # 16 hex + 2 quotes

    def test_missing_size_stable(self):
        r = {"token": "abc"}
        assert compute_etag(r) == compute_etag(r)

    def test_stable_across_file_id_changes(self):
        """ETag must NOT depend on file_id so it survives reference refreshes."""
        r1 = {"token": "abc", "size_bytes": 1000, "file_id": "old_ref"}
        r2 = {"token": "abc", "size_bytes": 1000, "file_id": "new_ref"}
        assert compute_etag(r1) == compute_etag(r2)


# ═══════════════════════════════════════════════════════════════════════════════
# media_info
# ═══════════════════════════════════════════════════════════════════════════════
class TestMediaInfo:
    def test_document_with_name(self):
        msg = {"document": {"file_id": "d1", "file_name": "report.pdf"}}
        media, name = media_info(msg)
        assert media is not None
        assert name == "report.pdf"

    def test_video_adds_mp4(self):
        msg = {"video": {"file_id": "v1"}}
        _, name = media_info(msg)
        assert name.endswith(".mp4")

    def test_audio_adds_mp3(self):
        msg = {"audio": {"file_id": "a1"}}
        _, name = media_info(msg)
        assert name.endswith(".mp3")

    def test_voice_adds_ogg(self):
        msg = {"voice": {"file_id": "v1"}}
        _, name = media_info(msg)
        assert name.endswith(".ogg")

    def test_animation_adds_mp4(self):
        msg = {"animation": {"file_id": "a1"}}
        _, name = media_info(msg)
        assert name.endswith(".mp4")

    def test_sticker_adds_webp(self):
        msg = {"sticker": {"file_id": "s1"}}
        _, name = media_info(msg)
        assert name.endswith(".webp")

    def test_photo_picks_largest(self):
        msg = {
            "photo": [
                {"file_id": "small", "file_size": 100},
                {"file_id": "large", "file_size": 9000},
                {"file_id": "mid", "file_size": 2000},
            ]
        }
        media, name = media_info(msg)
        assert media["file_id"] == "large"
        assert name == "telegram-photo.jpg"

    def test_no_media(self):
        media, name = media_info({"text": "hi"})
        assert media is None
        assert name == ""

    def test_document_preserves_extension(self):
        msg = {"document": {"file_id": "d", "file_name": "data.csv"}}
        _, name = media_info(msg)
        assert name == "data.csv"
