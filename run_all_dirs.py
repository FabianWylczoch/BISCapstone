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
                handlerA = f.read()
            with open(file2, "r") as f:
                handlerB = f.read()

            soupA = bs(handlerA, "lxml-xml")
            list_of_all_idsA = soupA.find_all("SHORT-NAME")
            soupB = bs(handlerB, "lxml-xml")
            list_of_all_idsB = soupB.find_all("SHORT-NAME")

            list_of_all_idsA = [id.getText() for id in list_of_all_idsA]
            list_of_all_idsB = [id.getText() for id in list_of_all_idsB]

            dfA = pd.DataFrame({"idsA": list_of_all_idsA})
            dfB = pd.DataFrame({"idsB": list_of_all_idsB})
            df_merged = pd.merge(dfA, dfB, how="outer", left_on="idsA", right_on="idsB")
            df_merged["type"] = "unchanged"
            df_merged["type"][df_merged.idsB.isna()] = "deleted"
            df_merged["type"][df_merged.idsA.isna()] = "new"
            df_merged["previous_year"] = year1
            df_merged["current_year"] = year2
            df_merged["document_name"] = file
            df = df_merged.copy()
            df["Text Release A"] = ""
            df["Text Release B"] = ""

            for row in df.iterrows():
                if (soupA.find("SHORT-NAME", string=row[1][0])) and (
                    not pd.isna(row[1][0])
                ):
                    df["Text Release A"].iloc[row[0]] = [
                        x.getText()
                        for x in soupA.find(
                            "SHORT-NAME", string=row[1][0]
                        ).parent.find_all("L-1")
                    ]

                # soup = soupB
                # for row in df.iterrows():
                if (soupB.find("SHORT-NAME", string=row[1][1])) and (
                    not pd.isna(row[1][1])
                ):
                    df["Text Release B"].iloc[row[0]] = [
                        x.getText()
                        for x in soupB.find(
                            "SHORT-NAME", string=row[1][1]
                        ).parent.find_all("L-1")
                    ]
            # df_merged_2 = df.copy()
            # df_merged_2 = df_merged_2[df_merged_2["type"] == "unchanged"]
            # df_merged_2["Difference in String"] = (
            #     df_merged_2["Text Release A"] != df_merged_2["Text Release B"]
            # )

            # no_changed = df_merged_2['Difference in String'][df_merged_2['Difference in String']==True].count()
            # no_total = df_merged_2[df_merged_2['type']=='unchanged']['Difference in String'].count()
            df.to_pickle(f"./data/df_{file}.pkl")
            help_list.append(df)


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
