"""Unit tests for the reply chunker."""

from __future__ import annotations

import pytest

from bt_signal_gateway.chunking import chunk_message


def test_empty_and_whitespace_return_no_chunks() -> None:
    assert chunk_message("", 100) == []
    assert chunk_message("   \n\n  ", 100) == []


def test_short_text_is_a_single_chunk() -> None:
    assert chunk_message("hello world", 100) == ["hello world"]


def test_invalid_max_length_raises() -> None:
    with pytest.raises(ValueError):
        chunk_message("hi", 0)


def test_every_chunk_within_limit() -> None:
    text = "\n\n".join(f"Paragraph {i} has several words in it." for i in range(20))
    chunks = chunk_message(text, 50)
    assert len(chunks) > 1
    assert all(len(chunk) <= 50 for chunk in chunks)


def test_paragraphs_join_with_blank_line_when_they_fit() -> None:
    text = "First para.\n\nSecond para."
    assert chunk_message(text, 100) == ["First para.\n\nSecond para."]


def test_long_paragraph_splits_on_sentence_boundaries() -> None:
    text = "Alpha sentence one. Beta sentence two. Gamma sentence three."
    chunks = chunk_message(text, 25)
    assert all(len(chunk) <= 25 for chunk in chunks)
    # Reassembled words survive the split (no characters dropped within words).
    assert "Alpha" in chunks[0]
    assert any("Gamma" in chunk for chunk in chunks)


def test_word_longer_than_limit_is_hard_split() -> None:
    word = "x" * 25
    chunks = chunk_message(word, 10)
    assert chunks == ["x" * 10, "x" * 10, "x" * 5]


def test_crlf_normalized() -> None:
    assert chunk_message("a\r\n\r\nb", 100) == ["a\n\nb"]
