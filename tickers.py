"""
tickers.py — Predetermined TSX / TSXV small-cap energy & mining universe.

Edit this list freely. No scraping, no API calls — you own the list.
Organised by sector so it is easy to enable/disable whole groups.

Naming convention:
  .TO  = Toronto Stock Exchange (TSX)
  .V   = TSX Venture Exchange (TSXV)
"""

# ---------------------------------------------------------------------------
# Oil & Gas — TSX
# ---------------------------------------------------------------------------
OIL_GAS_TSX = [
    "SGY.TO",   # Surge Energy
    # "GXE.TO",   # Gear Energy — delisted
    "ATH.TO",   # Athabasca Oil
    "TVE.TO",   # Tamarack Valley Energy
    "CR.TO",    # Crew Energy
    "AAV.TO",   # Advantage Energy
    "PEY.TO",   # Peyto Exploration & Development
    "BTE.TO",   # Baytex Energy
    "WCP.TO",   # Whitecap Resources
    "ARX.TO",   # ARC Resources
    "TOU.TO",   # Tourmaline Oil
    "VET.TO",   # Vermilion Energy
    "CPG.TO",   # Crescent Point Energy
    "ERF.TO",   # Enerplus
    "MEG.TO",   # MEG Energy
    "TVE.TO",   # Tamarack Valley Energy
    "POU.TO",   # Paramount Resources
    "NVA.TO",   # NuVista Energy
    "BIR.TO",   # Birchcliff Energy
    "KEL.TO",   # Kelt Exploration
    "PKI.TO",   # Parkland Corp (downstream)
    "CJ.TO",    # Cardinal Energy
    "FRU.TO",   # Freehold Royalties
]

# ---------------------------------------------------------------------------
# Oil & Gas — TSXV
# ---------------------------------------------------------------------------
OIL_GAS_TSXV = [
    "PBH.V",    # Premium Brands (check)
    "MAKO.V",   # Mako Energy
]

# ---------------------------------------------------------------------------
# Gold & Silver — TSX
# ---------------------------------------------------------------------------
GOLD_SILVER_TSX = [
    "EQX.TO",   # Equinox Gold
    "BTO.TO",   # B2Gold
    "GGD.TO",   # GoldMining Inc
    "TLG.TO",   # Triumph Gold
    "FF.TO",    # First Mining Gold
    "DSV.TO",   # Discovery Silver
    "VZLA.TO",  # Vizsla Silver
    "USA.TO",   # Americas Gold and Silver
    "ORE.TO",   # Orezone Gold
    "AAG.TO",   # Aftermath Silver (check suffix)
    "MTA.TO",   # Metalla Royalty
    "DML.TO",   # Denison Mines (uranium/gold)
    "LUG.TO",   # Lundin Gold
    "ABX.TO",   # Barrick (large-cap, include for Nuttall-type signals)
    "AG.TO",    # First Majestic Silver
    "IAU.TO",   # i-80 Gold
    "NG.TO",    # Novagold Resources
]

# ---------------------------------------------------------------------------
# Gold & Silver — TSXV
# ---------------------------------------------------------------------------
GOLD_SILVER_TSXV = [
    "SAG.V",    # Scout AI Gold (mentioned in original conversation — drill results Sep 29)
    "GSVR.V",   # Guanajuato Silver
    "VZLA.V",   # Vizsla Resources (if still on TSXV)
    "GR.V",     # Goldstrike Resources
    "SPA.V",    # Spanish Mountain Gold
]

# ---------------------------------------------------------------------------
# Copper & Base Metals — TSX / TSXV
# ---------------------------------------------------------------------------
COPPER_BASE_TSX = [
    "ASCU.TO",  # Arizona Sonoran Copper
    "CS.TO",    # Capstone Copper (mid-cap)
    "HBM.TO",   # Hudbay Minerals
    "FM.TO",    # First Quantum Minerals
    "LUN.TO",   # Lundin Mining
]

COPPER_BASE_TSXV = [
    "SOIL.TO",  # Soil Technologies / copper (verify)
]

# ---------------------------------------------------------------------------
# Uranium
# ---------------------------------------------------------------------------
URANIUM = [
    "NXE.TO",   # NexGen Energy
    "DML.TO",   # Denison Mines
    "FCU.TO",   # Fission Uranium
    "UEC.TO",   # Uranium Energy (cross-listed)
]

# ---------------------------------------------------------------------------
# Master list — everything above combined
# Duplicates are removed automatically.
# ---------------------------------------------------------------------------
TICKERS: list[str] = list(dict.fromkeys(
    OIL_GAS_TSX
    + OIL_GAS_TSXV
    + GOLD_SILVER_TSX
    + GOLD_SILVER_TSXV
    + COPPER_BASE_TSX
    + COPPER_BASE_TSXV
    + URANIUM
))


if __name__ == "__main__":
    print(f"Total tickers: {len(TICKERS)}")
    for t in TICKERS:
        print(f"  {t}")
