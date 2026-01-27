import pandas as pd
import re

def clean_number(val):
    if val is None:
        return None
    val = str(val)
    val = re.sub(r"[₹,%]", "", val)
    val = val.replace(",", "").strip()
    return float(val) if val.replace(".", "", 1).isdigit() else None


def extract_quarterly_results(soup, ticker: str):
    section = soup.find("section", id="quarters")
    if not section:
        return None

    table = section.find("table")
    df = pd.read_html(str(table))[0]

    df = df.rename(columns={df.columns[0]: "metric"})
    melted = df.melt(
        id_vars="metric",
        var_name="quarter",
        value_name="value"
    )

    melted["value"] = melted["value"].apply(clean_number)
    melted["ticker"] = ticker

    return melted
