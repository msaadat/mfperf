import string
import requests
from datetime import datetime, timedelta
from urllib.parse import quote

import pandas as pd
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
}

def encode_date(dt):
    return quote(dt.strftime("%m/%d/%Y"), safe="")


def mufap_cat_annualize(cat):
    ann_cats = ["Income", "Money Market", "Fixed Rate", "Debt"]

    for c in ann_cats:
        if c in cat:
            return True
    return False


def mufap_get_nav_tab(cat_id="", mufap_tab=""):
    mufap_tabs = {"funds": "01", "etf": "05", "vps": "02", "dedicated": "04"}
    # dedicated funds
    if cat_id != "":
        if cat_id in ["37", "38"]:
            return "04"
        # etf
        elif cat_id in ["42", "43", "46"]:
            return "05"

    if mufap_tab != "":
        return mufap_tabs[mufap_tab]

    return "01"


def mufap_fund_navs(start_date, fund_id="", fund_cat="", mufap_tab="", full=False):
    date_enc = encode_date(start_date)

    nav_tab = mufap_get_nav_tab(fund_cat, mufap_tab)

    url = f"https://www.mufap.com.pk/nav-report.php?tab={nav_tab}&amc=&fname={fund_id}&cat=&strdate={date_enc}&endate=&submitted=Show+Report"

    r = requests.get(url)

    df_nav = pd.read_html(r.text)[0]
    df_nav.columns = df_nav.iloc[0]

    # etf
    if nav_tab == "05" or nav_tab == "02":
        df_nav["Class"] = "-"
        df_nav["Type"] = "-"

    df_nav = mufap_nav_adjust_name(df_nav)
    df_nav = df_nav.iloc[1:]

    df_nav["Validity Date"] = pd.to_datetime(
        df_nav["Validity Date"], format="%b %d, %Y", errors="coerce"
    )
    df_nav = df_nav.dropna(axis=0, subset=["Validity Date"])

    if not full:
        df_nav = df_nav[["Fund Name", "NAV", "Validity Date"]]

    return df_nav


def mufap_fund_payouts(start_date, fund_id="", fund_cat=""):
    date_enc = encode_date(start_date)
    nav_tab = mufap_get_nav_tab(fund_cat)
    url = f"https://www.mufap.com.pk/payout-report.php?tab={nav_tab}&amc=&fname={fund_id}&cat=&strdate={date_enc}&endate=&submitted=Show+Report"

    r = requests.get(url)

    df = pd.read_html(r.text)[0]
    df.columns = df.iloc[0]
    df = df.iloc[1:][["Fund Name", "Payout (Per Unit)", "Ex-NAV", "Payout Date"]]

    df["Payout Date"] = pd.to_datetime(
        df["Payout Date"], format="%b %d, %Y", errors="coerce"
    )
    df = df.dropna(axis=0, subset=["Payout Date"])

    return df


def mufap_nav_adjust_name(df_nav):
    df_nav["Fund Name"] = df_nav["Fund Name"].str.replace(r"\s+", " ", regex=True)
    df_nav["Fund Name"] = df_nav.apply(
        lambda row: row["Fund Name"] + "-" + row["Class"]
        if row["Class"] != "-"
        else row["Fund Name"],
        axis=1,
    )
    df_nav["Fund Name"] = df_nav.apply(
        lambda row: row["Fund Name"] + "--" + row["Type"]
        if row["Type"] != "-"
        else row["Fund Name"],
        axis=1,
    )
    return df_nav


def mufap_funds_list():
    urls = {
        "funds": "https://www.mufap.com.pk/nav-report.php?tab=01",
        "etf": "https://www.mufap.com.pk/nav-report.php?tab=05",
        "vps": "https://www.mufap.com.pk/nav-report.php?tab=02",
        "dedicated": "https://www.mufap.com.pk/nav-report.php?tab=04",
    }

    df_list = []
    content = ""

    for nav_type, url in urls.items():
        r = requests.get(url)

        df_nav = pd.read_html(r.text)[0]
        df_nav.columns = df_nav.iloc[0]
        df_nav = df_nav.iloc[1:]

        if nav_type == "etf":
            df_nav = df_nav.iloc[:, :-4]
        if nav_type == "vps":
            df_nav = df_nav.iloc[:, :-5]
        if nav_type in ["etf", "vps"]:
            df_nav["Class"] = "-"
            df_nav["Type"] = "-"
        if nav_type in ["funds", "dedicated"]:
            df_nav.drop(["Offer", "Repurchase"], axis=1)

        # remove amc name row
        df_nav = df_nav[df_nav["Fund Name"] != df_nav["Category"]]
        df_nav["mufap_tab"] = nav_type

        df_nav["Validity Date"] = pd.to_datetime(
            df_nav["Validity Date"], format="%b %d, %Y", errors="coerce"
        )
        current_date = df_nav.groupby("Validity Date").count().idxmax()["Fund Name"]
        df_nav["backward"] = df_nav["Validity Date"].apply(
            lambda x: 1 if x > current_date else 0
        )

        content = r.content

        # build amc df
        soup = BeautifulSoup(content, "lxml")
        tbl = soup.find("table", class_="mydata")
        trs = tbl.find_all("tr")
        amc = ""
        lst = []
        for i in trs:
            td = i.find("td")
            if "amc" in td["class"]:
                amc = "".join(
                    filter(lambda x: x in string.printable, str(td.string).strip())
                )
                amc = " ".join(amc.split())
            elif "fundname" in td["class"]:
                fundname = "".join(
                    filter(lambda x: x in string.printable, str(td.string).strip())
                )
                fundname = " ".join(fundname.split())
                lst.append([fundname, amc])

        df_amcs = pd.DataFrame(lst)
        df_amcs.columns = ["Fund Name", "amc"]

        df_amc_ids = mufap_amc_list(txt=content)
        df_amcs = pd.merge(df_amcs, df_amc_ids, on="amc", how="left")
        # df_amcs.to_excel("df_amcs.xlsx")

        df_cats = mufap_category_list(txt=content)
        df_nav = pd.merge(df_nav, df_amcs, on="Fund Name", how="left").drop_duplicates()
        df_nav = pd.merge(
            df_nav, df_cats, left_on="Category", right_on="category", how="left"
        )

        df_list.append(df_nav)

    df_nav = pd.concat(df_list)

    df_nav = mufap_nav_adjust_name(df_nav)
    df_nav = df_nav.drop_duplicates()
    # df_nav.to_excel("df_nav.xlsx")

    # find fund ids
    df_ids = mufap_options_todf("Fund Name:", txt=content)
    df_ids.columns = ["fund_id", "Fund Name"]
    # df_ids.to_excel('df_ids.xlsx')

    df = pd.merge(
        df_ids,
        df_nav[
            [
                "Fund Name",
                "cat_id",
                "Inception Date",
                "amc_id",
                "backward",
                "mufap_tab",
                "Validity Date",
            ]
        ],
        on="Fund Name",
        how="left",
    )
    df["Inception Date"] = pd.to_datetime(
        df["Inception Date"], format="%b %d, %Y", errors="coerce"
    )
    df = df.dropna(subset=["Inception Date"])
    df = df.drop_duplicates(subset=["fund_id"])

    # correct vps category ids
    for idx, row in df[df["mufap_tab"] == "vps"].iterrows():
        df_nav = mufap_fund_navs(
            row["Validity Date"] - timedelta(days=10),
            row["fund_id"],
            mufap_tab="vps",
            full=True,
        )
        if df_nav.shape[0] > 0:
            cat_id = df_cats[df_cats["category"] == df_nav["Category"].iloc[0]][
                "cat_id"
            ].iloc[0]
            df.at[idx, "cat_id"] = cat_id

    df = df.drop(columns=["Validity Date"])

    return df


def mufap_amc_list(url="https://www.mufap.com.pk/nav-report.php?tab=01", txt=None):
    df_amc_ids = mufap_options_todf("AMC:", url=url, txt=txt)
    df_amc_ids.columns = ["amc_id", "amc"]
    df_amc_ids["amc"] = df_amc_ids["amc"].apply(
        lambda x: "".join(
            filter(lambda y: y in string.printable, " ".join(x.split()).strip())
        )
    )
    return df_amc_ids


def mufap_category_list(url="https://www.mufap.com.pk/nav-report.php?tab=01", txt=None):
    df_cat_ids = mufap_options_todf("Category:", url=url, txt=txt)
    df_cat_ids.columns = ["cat_id", "category"]
    df_cat_ids["category"] = df_cat_ids["category"].apply(
        lambda x: "".join(
            filter(lambda y: y in string.printable, " ".join(x.split()).strip())
        )
    )
    return df_cat_ids


def mufap_options_todf(find_str, url=None, txt=None):
    if not txt:
        r = requests.get(url)
        txt = r.content

    soup = BeautifulSoup(txt, "lxml")
    p_tag = soup.find(string=find_str).find_parent()
    options = p_tag.find_all("option")

    option_values = []
    option_texts = []
    for i in options:
        value = i.get("value")
        text = i.text.strip()
        option_values.append(value)
        option_texts.append(text)

    data = {"id": option_values, "value": option_texts}
    df = pd.DataFrame(data).dropna()
    df["value"] = df["value"].str.replace(r"\s+", " ", regex=True)

    return df


if __name__ == "__main__":
    # df = mufap_options_todf("AMC:","https://www.mufap.com.pk/nav-report.php?tab=01")
    # df.to_excel('amcs.xlsx')
    df = mufap_funds_list()
    df.to_excel("df.xlsx")

    # print(df[df['fund_id']=='B152'])
