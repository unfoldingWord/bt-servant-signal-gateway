"""Split long replies into Signal-sized chunks.

Signal (like the other channels) caps how much text fits in one message, so a
long worker reply is split into several messages. The split prefers natural
boundaries: paragraphs first, then sentences, then words, and only as a last
resort cuts mid-word.

Ported from ``../bt-servant-telegram-gateway/src/services/chunking.ts``
(``chunkMessage`` / ``expandParagraph``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_PARAGRAPH_SPLIT = re.compile(r"\n{2,}")
_WHITESPACE_SPLIT = re.compile(r"\s+")
# A sentence: run of non-terminator chars ending in one-or-more .!?, or a
# trailing run with no terminator at end of paragraph.
_SENTENCE_SPLIT = re.compile(r"[^.!?]+[.!?]+|[^.!?]+$")
_SENTENCE_PUNCT = re.compile(r"[.!?]")


@dataclass(frozen=True, slots=True)
class _Segment:
    text: str
    # How this segment rejoins the previous one when they fit together.
    separator: str  # "blank" -> "\n\n", "space" -> " "


def chunk_message(text: str, max_length: int) -> list[str]:
    """Split *text* into chunks no longer than *max_length* characters.

    Greedily packs paragraph/sentence/word segments together, emitting a new
    chunk whenever the next segment would overflow. Returns ``[]`` for empty or
    whitespace-only input. Raises ``ValueError`` if *max_length* < 1.
    """
    if max_length <= 0:
        raise ValueError("max_length must be greater than 0")

    normalized = text.replace("\r\n", "\n").strip()
    if not normalized:
        return []

    paragraphs = [p.strip() for p in _PARAGRAPH_SPLIT.split(normalized) if p.strip()]
    segments: list[_Segment] = []
    for paragraph in paragraphs:
        segments.extend(_expand_paragraph(paragraph, max_length))

    chunks: list[str] = []
    current = ""
    for segment in segments:
        if current:
            joiner = "\n\n" if segment.separator == "blank" else " "
            candidate = f"{current}{joiner}{segment.text}"
        else:
            candidate = segment.text

        if len(candidate) <= max_length:
            current = candidate
            continue

        # Candidate overflows: flush what we have and start fresh.
        if current:
            chunks.append(current)
            current = ""

        if len(segment.text) <= max_length:
            current = segment.text
            continue

        # A single segment longer than max_length: split it word by word, and
        # hard-split any word that is itself too long.
        current = _split_oversized_segment(segment.text, max_length, chunks)

    if current:
        chunks.append(current)

    return [chunk for chunk in chunks if chunk]


def _expand_paragraph(paragraph: str, max_length: int) -> list[_Segment]:
    """Break a paragraph into segments that each (ideally) fit *max_length*.

    A short paragraph stays whole; a long one is split into sentences, falling
    back to words when it has no sentence punctuation.
    """
    if len(paragraph) <= max_length:
        return [_Segment(paragraph, "blank")]

    if _SENTENCE_PUNCT.search(paragraph):
        sentences = [s.strip() for s in _SENTENCE_SPLIT.findall(paragraph) if s.strip()]
        if sentences:
            return [
                _Segment(text, "blank" if i == 0 else "space") for i, text in enumerate(sentences)
            ]

    words = _WHITESPACE_SPLIT.split(paragraph)
    return [_Segment(word, "blank" if i == 0 else "space") for i, word in enumerate(words)]


def _split_oversized_segment(segment_text: str, max_length: int, chunks: list[str]) -> str:
    """Pack words of an over-long segment into *chunks*; return the trailing run.

    Words that exceed *max_length* on their own are hard-split. Appends finished
    chunks to *chunks* in place and returns the not-yet-flushed remainder.
    """
    word_chunk = ""
    for word in _WHITESPACE_SPLIT.split(segment_text):
        candidate = f"{word_chunk} {word}" if word_chunk else word
        if len(candidate) <= max_length:
            word_chunk = candidate
            continue

        if word_chunk:
            chunks.append(word_chunk)
            word_chunk = ""

        if len(word) > max_length:
            for i in range(0, len(word), max_length):
                piece = word[i : i + max_length]
                if len(piece) == max_length:
                    chunks.append(piece)
                else:
                    word_chunk = piece
        else:
            word_chunk = word

    return word_chunk
