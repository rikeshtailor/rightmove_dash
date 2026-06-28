"""Identify HMO (C3 -> C4) Article 4 directions within the national dataset.

planning.data.gov.uk's `article-4-direction-area` dataset mixes every kind of
Article 4 direction together: conservation-area window/boundary controls,
demolition, shopfronts, agricultural restrictions, and the one an HMO investor
cares about -- the change of use from a dwellinghouse (Use Class C3) to a small
HMO (Use Class C4). This module decides whether a given area record is an HMO one.

Council data is inconsistent, so we use two independent signals and treat a
record as HMO if EITHER fires:

  1. `permitted-development-rights` -- when populated, names the rights the
     direction withdraws. The C3<->C4 change is Part 3, Class L of the GPDO 2015.
  2. Free text (`name`, `notes`, `description`) -- very often says "HMO", "C4"
     or "houses in multiple occupation".

The bias is deliberately inclusive. A false positive (flagging a non-HMO area as
HMO) is easy to spot and correct; a false negative silently tells someone a
street is clear for permitted development when it is not. Tune the token lists
below against your ingested data using the coverage log that ingest.py prints.
"""
from __future__ import annotations

# Tokens in `permitted-development-rights` indicating the C3<->C4 (small HMO)
# change of use. Matching ignores case, spaces, hyphens and underscores.
HMO_PD_RIGHT_TOKENS = (
    "3l",            # Part 3 Class L in compact form (e.g. "3L")
    "part3classl",
    "classlc4",
)

# Text fragments in name/notes/description indicating an HMO direction.
HMO_TEXT_TOKENS = (
    "hmo",
    "house in multiple occupation",
    "houses in multiple occupation",
    "multiple occupation",
    "c3 to c4",
    "c4",
    "small hmo",
)

# Fragments that usually indicate a NON-HMO direction. Used only to annotate the
# coverage log (so you can audit borderline records), never to exclude outright.
NON_HMO_HINTS = (
    "conservation",
    "shopfront",
    "shop front",
    "demolition",
    "agricultural",
    "telecommunication",
    "fenestration",
    "boundary wall",
)


def _norm(value) -> str:
    return "" if value is None else str(value).lower()


def _squash(value: str) -> str:
    return value.replace(" ", "").replace("-", "").replace("_", "")


def matches_hmo_pd_right(pd_rights) -> bool:
    squashed = _squash(_norm(pd_rights))
    return any(tok in squashed for tok in HMO_PD_RIGHT_TOKENS)


def matches_hmo_text(*text_fields) -> bool:
    blob = " ".join(_norm(t) for t in text_fields)
    return any(tok in blob for tok in HMO_TEXT_TOKENS)


def is_hmo_article4(props: dict) -> bool:
    """True if this article-4-direction-area record looks like an HMO (C3->C4) one."""
    pd_rights = props.get("permitted-development-rights") or props.get(
        "permitted_development_rights"
    )
    return matches_hmo_pd_right(pd_rights) or matches_hmo_text(
        props.get("name"), props.get("notes"), props.get("description")
    )


def looks_non_hmo(props: dict) -> bool:
    """Annotation for the coverage log: text smells like a non-HMO direction."""
    blob = " ".join(_norm(props.get(k)) for k in ("name", "notes", "description"))
    return any(h in blob for h in NON_HMO_HINTS) and not matches_hmo_text(blob)
