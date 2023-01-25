from bs4 import BeautifulSoup as bs
import lxml
import pandas as pd
import os


def compare_years(year1, year2, help_list):
    path = "UC 3/etl_generated/"
    folder1 = f"{path}/{year1}"
    folder2 = f"{path}/{year2}"

    # Comparing files in year folder
    all_files_year1 = os.listdir(folder1)
    all_files_year2 = os.listdir(folder2)
    if not all_files_year1 == all_files_year2:
        raise ValueError(f"FolderList not the same for {year1} and {year2}")

    for folder in os.listdir(folder1):
        for file in os.listdir(f"{folder1}/{folder}/zz_generated"):
            if file not in os.listdir(f"{folder2}/{folder}/zz_generated"):
                raise ValueError(f"{file} not in {folder2}/{folder}/zz_generated")

            ## Compare files
            file1 = f"{folder1}/{folder}/zz_generated/{file}"
            file2 = f"{folder2}/{folder}/zz_generated/{file}"
            print(f"Comapring {file1} and {file2}")
            with open(file1, "r") as f:
                data1 = f.read()
            with open(file2, "r") as f:
                data2 = f.read()

            list_of_all_ids1 = bs(data1, "lxml-xml").find_all("SHORT-NAME")
            list_of_all_ids2 = bs(data2, "lxml-xml").find_all("SHORT-NAME")
            df1 = pd.DataFrame({"previous_ids": list_of_all_ids1})
            df2 = pd.DataFrame({"current_ids": list_of_all_ids2})
            df_merged = pd.merge(
                df1, df2, how="outer", left_on="previous_ids", right_on="current_ids"
            )
            df_merged["type"] = "unchanged"
            df_merged["type"][df_merged.current_ids.isna()] = "delete"
            df_merged["type"][df_merged.previous_ids.isna()] = "new"
            df_merged["previous_year"] = year1
            df_merged["current_year"] = year2
            df_merged["document_name"] = file
            help_list.append(df_merged)


def run_all_dirs(years):
    # years = ["R20-11", "R21-11", "R22-11"]
    path = "UC 3/etl_generated/"
    folder1 = f"{path}/R20-11"
    folder2 = f"{path}/R21-11"
    help_list = []

    for idx in range(0, len(years)):
        if len(years) > idx + 1:
            print(compare_years(years[idx], years[idx + 1], help_list))
    return pd.concat(help_list)
