import pandas as pd
import re

def clean_number(value):
    if value is None:
        return None
    value = str(value)
    value = re.sub(r"[₹,%]", "", value)
    value = value.replace(",", "").strip()
    return float(value) if value.replace(".", "", 1).isdigit() else None


def extract_annual_table(soup, section_id: str, ticker: str):
    """
    Generic extractor for:
    - profit-loss
    - balance-sheet
    - cash-flow
    """
    section = soup.find("section", id=section_id)
    if not section:
        return None

    table = section.find("table")
    if not table:
        return None

    df = pd.read_html(str(table))[0]

    # Normalize to LONG format
    df = df.rename(columns={df.columns[0]: "metric"})
    melted = df.melt(
        id_vars="metric",
        var_name="fiscal_year",
        value_name="value"
    )

    melted["value"] = melted["value"].apply(clean_number)
    melted["ticker"] = ticker

    return melted
