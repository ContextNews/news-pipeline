"""Integration test that exercises Cronkite with real model calls."""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone
import json
import pprint

import pytest
from dotenv import load_dotenv

from generate_stories.generate_stories import generate_story


load_dotenv()


@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY is required to run Cronkite integration test",
)
def test_generate_story_with_cronkite() -> None:
    cluster = [
        {
            "id": "reuters-001",
            "title": "EU and US leaders meet in Geneva to discuss trade framework",
            "summary": "Leaders from the European Union and United States met in Geneva to outline a new transatlantic trade framework.",
            "text": "Leaders from the European Union and United States met on Friday at the Palais des Nations in Geneva, Switzerland to outline a new transatlantic trade framework focused on tariffs, digital services, and semiconductor supply chains. EU Trade Commissioner Valdis Dombrovskis and US Trade Representative Katherine Tai held bilateral talks on the sidelines of the World Trade Organization meetings.",
            "published_at": datetime(2024, 3, 15, 9, 30, tzinfo=timezone.utc),
            "source": "Reuters",
        },
        {
            "id": "ap-002",
            "title": "Geneva summit focuses on transatlantic trade priorities",
            "summary": "The Geneva summit discussed priorities for a US-EU trade accord and next steps for negotiations.",
            "text": "At the Geneva summit, officials from Brussels and Washington discussed timelines for a comprehensive trade accord and unresolved issues on agricultural tariffs and data privacy standards. Swiss Foreign Minister Ignazio Cassis welcomed delegates to the historic lakeside venue, noting Geneva's long tradition of hosting international trade negotiations.",
            "published_at": datetime(2024, 3, 15, 12, 0, tzinfo=timezone.utc),
            "source": "AP",
        },
    ]

    print("\nDemo articles:")
    pprint.pprint(cluster)
    story = generate_story(cluster, model="gpt-4o-mini")

    print("\nGenerated story:")
    pprint.pprint(story)

    json.dumps(asdict(story))

    cluster_two = [
        {
            "id": "reuters-101",
            "title": "Federal Reserve signals rate pause amid cooling inflation",
            "summary": "The Federal Reserve indicated it may pause rate hikes after recent inflation data softened.",
            "text": "At a press conference at the Federal Reserve headquarters in Washington, D.C., Chair Jerome Powell said recent inflation prints show cooling trends, suggesting a possible pause in rate hikes. The Fed held its benchmark rate steady at 5.25-5.50% following the two-day FOMC meeting.",
            "published_at": datetime(2024, 4, 2, 14, 0, tzinfo=timezone.utc),
            "source": "Reuters",
        },
        {
            "id": "ft-102",
            "title": "Wall Street rallies as Fed hints at holding rates",
            "summary": "US markets rose after the Federal Reserve hinted at holding interest rates steady.",
            "text": "Stocks climbed on Wall Street and across major exchanges after Fed policymakers signaled they could hold rates steady if inflation continues to ease. The S&P 500 gained 1.2% and the Dow Jones Industrial Average rose 350 points in afternoon trading in New York.",
            "published_at": datetime(2024, 4, 2, 15, 30, tzinfo=timezone.utc),
            "source": "Financial Times",
        },
        {
            "id": "ap-103",
            "title": "US businesses plan for stable borrowing costs after Fed meeting",
            "summary": "American executives said stable borrowing costs would aid investment planning.",
            "text": "Several executives at the US Chamber of Commerce in Washington said stable borrowing costs would improve investment planning as demand steadies. Manufacturing firms in the Midwest reported renewed optimism following the Fed's announcement.",
            "published_at": datetime(2024, 4, 2, 16, 10, tzinfo=timezone.utc),
            "source": "AP",
        },
        {
            "id": "bloomberg-104",
            "title": "Treasury yields slip after Powell's comments",
            "summary": "US Treasury yields slipped following Chair Powell's comments about inflation progress.",
            "text": "US Treasury yields slipped after Chair Powell's comments at the Eccles Building highlighted progress on inflation and the possibility of holding rates. The 10-year Treasury yield fell to 4.35% from 4.42% earlier in the day.",
            "published_at": datetime(2024, 4, 2, 16, 40, tzinfo=timezone.utc),
            "source": "Bloomberg",
        },
        {
            "id": "wsj-105",
            "title": "Economists debate timing of Fed rate pause",
            "summary": "Economists debated whether the Federal Reserve would pause at its June meeting.",
            "text": "Economists surveyed by the Wall Street Journal said a pause is possible at the June FOMC meeting in Washington if inflation data remains soft. Goldman Sachs and JPMorgan analysts revised their rate forecasts following the announcement.",
            "published_at": datetime(2024, 4, 2, 17, 5, tzinfo=timezone.utc),
            "source": "WSJ",
        },
        {
            "id": "guardian-106",
            "title": "American households feel relief as mortgage rates stabilize",
            "summary": "US households reported relief as borrowing costs stabilized.",
            "text": "Households across the United States said the stabilization of mortgage rates provided relief after months of increases. Homebuyers in cities like Denver, Atlanta, and Phoenix reported renewed interest in the housing market.",
            "published_at": datetime(2024, 4, 2, 18, 0, tzinfo=timezone.utc),
            "source": "The Guardian",
        },
        {
            "id": "nyt-107",
            "title": "Fed officials stress data dependence for future moves",
            "summary": "Federal Reserve officials emphasized future moves depend on incoming data.",
            "text": "Federal Reserve officials in Washington emphasized that future policy moves depend on incoming inflation and labor market data. Vice Chair Philip Jefferson echoed Powell's remarks in a speech at the Brookings Institution.",
            "published_at": datetime(2024, 4, 2, 19, 15, tzinfo=timezone.utc),
            "source": "NYT",
        },
        {
            "id": "sports-999",
            "title": "Wizards win playoff game in overtime thriller at Capital One Arena",
            "summary": "The Washington Wizards won a playoff game in overtime after a late comeback.",
            "text": "The Washington Wizards rallied from a double-digit deficit to win the playoff game in overtime at Capital One Arena, celebrating with fans in downtown D.C.",
            "published_at": datetime(2024, 4, 2, 20, 0, tzinfo=timezone.utc),
            "source": "Local Sports",
        },
    ]

    print("\nDemo articles (cluster two):")
    pprint.pprint(cluster_two)
    story_two = generate_story(cluster_two, model="gpt-4o-mini")

    print("\nGenerated story (cluster two):")
    pprint.pprint(story_two)

    json.dumps(asdict(story_two))

    cluster_three = [
        {
            "id": "reuters-201",
            "title": "Miami approves $2 billion flood defense plan after Hurricane Ian damage",
            "summary": "Miami-Dade County approved a multi-year flood defense plan after Hurricane Ian caused extensive damage.",
            "text": "The Miami-Dade County Commission approved a $2 billion multi-year flood defense plan after Hurricane Ian caused extensive damage to coastal neighborhoods. Mayor Daniella Levine Cava said at City Hall, \"We have to build resilience now, not later. Miami's future depends on it.\"",
            "published_at": datetime(2024, 5, 8, 10, 0, tzinfo=timezone.utc),
            "source": "Reuters",
        },
        {
            "id": "ap-202",
            "title": "Miami Beach officials announce timeline for new sea wall project",
            "summary": "Miami Beach officials said construction on a new sea wall would begin later this year.",
            "text": "Miami Beach officials announced a timeline for the sea wall project along Ocean Drive and Collins Avenue, with construction expected to begin in the fall and finish within two years. The barrier will protect the Art Deco Historic District from storm surge.",
            "published_at": datetime(2024, 5, 8, 11, 15, tzinfo=timezone.utc),
            "source": "AP",
        },
        {
            "id": "bloomberg-203",
            "title": "Florida insurers adjust coverage after repeated Miami flooding",
            "summary": "Insurers said they would adjust coverage terms in South Florida's flood-prone areas.",
            "text": "After repeated flooding in South Florida, major insurers including Citizens Property Insurance said they would adjust coverage terms for properties in flood-prone areas of Miami-Dade and Broward counties. Some Brickell and Coconut Grove homeowners face premium increases of up to 40%.",
            "published_at": datetime(2024, 5, 8, 12, 30, tzinfo=timezone.utc),
            "source": "Bloomberg",
        },
        {
            "id": "nyt-204",
            "title": "Little Havana residents back relocation plan for high-risk areas",
            "summary": "Some Miami residents supported a voluntary relocation plan for high-risk neighborhoods.",
            "text": "At a public meeting at the Manuel Artime Community Center in Little Havana, residents backed a voluntary relocation plan. Maria Santos, a longtime resident, said, \"We want safer homes and a clear timeline. My family has flooded three times in five years.\"",
            "published_at": datetime(2024, 5, 8, 13, 0, tzinfo=timezone.utc),
            "source": "NYT",
        },
        {
            "id": "ft-205",
            "title": "Miami-Dade plans $500 million bond issuance to fund flood defenses",
            "summary": "Miami-Dade County plans to issue municipal bonds to fund flood defenses.",
            "text": "Miami-Dade County officials said they plan to issue $500 million in municipal bonds to fund flood defenses and related infrastructure upgrades across South Florida. The bonds will finance pump stations, elevated roads, and drainage improvements in vulnerable areas like Hialeah and Sweetwater.",
            "published_at": datetime(2024, 5, 8, 14, 10, tzinfo=timezone.utc),
            "source": "Financial Times",
        },
        {
            "id": "guardian-206",
            "title": "Everglades Foundation urges wetlands restoration as part of Miami flood plan",
            "summary": "Environmental groups urged Everglades wetlands restoration as part of the plan.",
            "text": "The Everglades Foundation and other environmental groups urged wetlands restoration to complement the sea wall project and reduce surge risks. Scientists said restoring mangroves in Biscayne Bay could absorb significant storm surge energy before it reaches Miami's urban core.",
            "published_at": datetime(2024, 5, 8, 15, 0, tzinfo=timezone.utc),
            "source": "The Guardian",
        },
        {
            "id": "sports-777",
            "title": "Inter Miami's star striker Messi returns in season opener at DRV PNK Stadium",
            "summary": "Lionel Messi returned in Inter Miami's season opener after injury.",
            "text": "Lionel Messi returned to the Inter Miami lineup in the MLS season opener at DRV PNK Stadium in Fort Lauderdale, scoring twice in a 3-1 win over Orlando City. The Argentine star had missed preseason with a hamstring issue.",
            "published_at": datetime(2024, 5, 8, 16, 0, tzinfo=timezone.utc),
            "source": "Local Sports",
        },
    ]

    print("\nDemo articles (cluster three):")
    pprint.pprint(cluster_three)
    story_three = generate_story(cluster_three, model="gpt-4o-mini")

    print("\nGenerated story (cluster three):")
    pprint.pprint(story_three)

    json.dumps(asdict(story_three))
