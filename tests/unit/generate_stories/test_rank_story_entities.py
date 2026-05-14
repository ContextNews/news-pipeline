"""Tests for ranking story entities by first appearance in title + summary."""

from __future__ import annotations

from generate_stories.rank_story_entities import rank_entities_by_appearance


class TestRankEntitiesByAppearance:
    def test_simple_ordering(self) -> None:
        text = "Biden meets Macron in Paris to discuss trade"
        ranks = rank_entities_by_appearance(
            text,
            {
                "Q6279": ["Joe Biden", "Biden"],
                "Q3052772": ["Emmanuel Macron", "Macron"],
                "Q90": ["Paris"],
            },
        )
        assert ranks == {"Q6279": 1, "Q3052772": 2, "Q90": 3}

    def test_no_match_means_no_rank(self) -> None:
        text = "Biden meets Macron in Paris"
        ranks = rank_entities_by_appearance(
            text,
            {
                "Q6279": ["Biden"],
                "Q99999": ["SomeoneElse"],
            },
        )
        assert ranks == {"Q6279": 1}
        assert "Q99999" not in ranks

    def test_case_insensitive(self) -> None:
        text = "BIDEN spoke with macron"
        ranks = rank_entities_by_appearance(
            text,
            {"Q6279": ["Biden"], "Q3052772": ["Macron"]},
        )
        assert ranks == {"Q6279": 1, "Q3052772": 2}

    def test_word_boundary_no_substring_match(self) -> None:
        # "China" should not match inside "Chinatown".
        text = "A celebration in Chinatown drew crowds"
        ranks = rank_entities_by_appearance(
            text,
            {"Q148": ["China"]},
        )
        assert ranks == {}

    def test_alias_match_when_canonical_absent(self) -> None:
        text = "POTUS announced new sanctions"
        ranks = rank_entities_by_appearance(
            text,
            {"Q6279": ["Joe Biden", "POTUS"]},
        )
        assert ranks == {"Q6279": 1}

    def test_diacritic_normalised_match(self) -> None:
        text = "Erdogan addressed the assembly"  # no diacritic
        ranks = rank_entities_by_appearance(
            text,
            {"Q220": ["Erdoğan"]},  # alias has diacritic
        )
        assert ranks == {"Q220": 1}

    def test_short_alias_skipped(self) -> None:
        # 2-letter aliases like "US" / "UN" can collide; the ranker skips them.
        text = "US troops withdrew from the region"
        ranks = rank_entities_by_appearance(
            text,
            {"Q30": ["US"]},
        )
        assert ranks == {}

    def test_earliest_alias_wins_for_qid(self) -> None:
        text = "POTUS made remarks. Later, Biden spoke again."
        ranks = rank_entities_by_appearance(
            text,
            {
                "Q6279": ["Biden", "POTUS"],
                "Q3052772": ["Macron"],
            },
        )
        # POTUS appears first, so Q6279 ranks 1.
        assert ranks == {"Q6279": 1}

    def test_empty_inputs(self) -> None:
        assert rank_entities_by_appearance("", {}) == {}
        assert rank_entities_by_appearance("some text", {}) == {}
        assert rank_entities_by_appearance("", {"Q1": ["foo"]}) == {}
