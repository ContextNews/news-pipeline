"""Mapping from story topics to World Bank time series indicator codes."""

from __future__ import annotations

TOPIC_INDICATORS: dict[str, list[str]] = {
    "Business": ["NY.GDP.MKTP.KD.ZG", "BX.KLT.DINV.WD.GD.ZS", "FP.CPI.TOTL.ZG"],
    "Conflict": ["MS.MIL.XPND.GD.ZS", "SM.POP.REFG.OR", "PV.EST", "NY.GDP.PCAP.CD"],
    "Crime": ["VC.IHR.PSRC.P5", "SI.POV.DDAY", "SP.URB.TOTL.IN.ZS", "SL.UEM.TOTL.ZS"],
    "Economy": ["NY.GDP.MKTP.CD", "NY.GDP.MKTP.KD.ZG", "FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS", "BN.CAB.XOKA.GD.ZS"],
    "Education": ["SE.XPD.TOTL.GD.ZS", "SE.PRM.ENRR", "SE.SEC.ENRR", "SE.ADT.LITR.ZS"],
    "Entertainment": ["IT.NET.USER.ZS", "SP.URB.TOTL.IN.ZS", "NY.GDP.PCAP.CD", "IT.CEL.SETS.P2"],
    "Environment": ["EN.ATM.CO2E.PC", "AG.LND.FRST.ZS", "EG.CFT.ACCS.ZS", "EG.FEC.RNEW.ZS"],
    "Geopolitics": ["MS.MIL.XPND.GD.ZS", "NE.TRD.GNFS.ZS", "SM.POP.NETM", "PV.EST", "BX.KLT.DINV.WD.GD.ZS"],
    "Health": ["SP.DYN.LE00.IN", "SH.DYN.MORT", "SH.XPD.CHEX.GD.ZS", "SH.MED.PHYS.ZS"],
    "Law": ["RL.EST", "VC.IHR.PSRC.P5", "GE.EST"],
    "Markets": ["FP.CPI.TOTL.ZG", "NY.GDP.MKTP.KD.ZG", "CM.MKT.LCAP.GD.ZS", "BX.KLT.DINV.WD.GD.ZS"],
    "Politics": ["VA.EST", "PV.EST", "GE.EST", "CC.EST"],
    "Science": ["GB.XPD.RSDV.GD.ZS", "SP.POP.SCIE.RD.P6", "TX.VAL.TECH.MF.ZS", "IP.PAT.RESD"],
    "Society": ["SI.POV.GINI", "SI.POV.DDAY", "SP.DYN.LE00.IN", "SM.POP.NETM"],
    "Sports": ["NY.GDP.PCAP.CD", "SP.URB.TOTL.IN.ZS", "SP.POP.TOTL", "SP.DYN.LE00.IN"],
    "Technology": ["IT.NET.USER.ZS", "IT.CEL.SETS.P2", "TX.VAL.TECH.MF.ZS", "GB.XPD.RSDV.GD.ZS"],
}


def get_indicators_for_topics(topics: list[str]) -> list[str]:
    """Return deduplicated WB indicator codes for a list of story topics.

    Preserves insertion order: first topic's indicators come first,
    then any new indicators from subsequent topics.
    """
    seen: set[str] = set()
    indicators: list[str] = []
    for topic in topics:
        for code in TOPIC_INDICATORS.get(topic.title(), []):
            if code not in seen:
                seen.add(code)
                indicators.append(code)
    return indicators
