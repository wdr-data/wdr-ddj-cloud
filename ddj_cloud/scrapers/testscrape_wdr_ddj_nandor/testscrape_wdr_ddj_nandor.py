
import pandas as pd

from ddj_cloud.utils.storage import upload_dataframe


url = "https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Nowcasting_und_-R-Schaetzung/main/Nowcast_R_aktuell.csv"


def run():
    # Parse into data frame
    df = pd.read_csv(url, sep=",", decimal=".", low_memory=False)

    upload_dataframe(df, "testscrape_wdr_ddj_nandor/rki_github_r.csv")

