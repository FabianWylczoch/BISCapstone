import pandas as pd
import os
from lxml import etree
from Levenshtein import distance as lev
from datetime import datetime


def prettifyString(string):
    return (
        " ".join(string)
        .replace("\n", " ")
        .replace("                                                          ", " ")
        .replace("    ", " ")
    )


def lev_distance(row):
    ## Calculate lev distance of df row
    if row[6] == "" and row[7] == "":
        return 0
    return lev(row[6], row[7]) / (max(len(row[6]), len(row[7])))


def jaccard_similarity(row):
    ## Calculate jaccard sim of a df row
    if row[6] == "" and row[7] == "":
        return 0
    # convert to set
    a = set(row[6])
    b = set(row[7])
    # calucate jaccard similarity
    j = float(len(a.intersection(b))) / len(a.union(b))
    return j


def compare_years(year1, year2, help_list):
    path = "UC 3/etl_generated/"
    folder1 = f"{path}/{year1}"
    folder2 = f"{path}/{year2}"

    # Comparing files in year folder
    all_files_year1 = os.listdir(folder1)
    all_files_year2 = os.listdir(folder2)
    if not all_files_year1 == all_files_year2:
        raise ValueError(f"FolderList not the same for {year1} and {year2}")
    date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    os.mkdir(f"./data/{date}")
    counter = 1
    for folder in os.listdir(folder1):
        for file in os.listdir(f"{folder1}/{folder}/zz_generated"):
            if file not in os.listdir(f"{folder2}/{folder}/zz_generated"):
                raise ValueError(f"{file} not in {folder2}/{folder}/zz_generated")

            ## Compare files
            file1 = f"{folder1}/{folder}/zz_generated/{file}"
            file2 = f"{folder2}/{folder}/zz_generated/{file}"
            # with open(file1, "r") as f:
            #     handlerA = f.read()
            # with open(file2, "r") as f:
            #     handlerB = f.read()
            treeA = etree.parse(
                file1
            )  # "UC 3/etl_generated/R21-11/SWS_CANInterface_012/zz_generated/AUTOSAR_SWS_CANInterface_resolved.arxml")
            treeB = etree.parse(
                file2
            )  # "UC 3/etl_generated/R22-11/SWS_CANInterface_012/zz_generated/AUTOSAR_SWS_CANInterface_resolved.arxml")

            idsA = treeA.xpath(
                f"//*[self::x:TRACE or self::x:TRACEABLE-TABLE]/x:SHORT-NAME/text()",
                namespaces={"x": "http://autosar.org/schema/r4.0"},
            )
            idsB = treeB.xpath(
                f"//*[self::x:TRACE or self::x:TRACEABLE-TABLE]/x:SHORT-NAME/text()",
                namespaces={"x": "http://autosar.org/schema/r4.0"},
            )

            dfA = pd.DataFrame({"idsA": idsA})
            dfB = pd.DataFrame({"idsB": idsB})
            df = pd.merge(dfA, dfB, how="outer", left_on="idsA", right_on="idsB")
            df["type"] = "unchanged"
            df["type"][df.idsB.isna()] = "deleted"
            df["type"][df.idsA.isna()] = "new"
            df["previous_year"] = year1
            df["current_year"] = year2
            df["document_name"] = file

            textA = []
            structureA = []
            for id in df["idsA"]:
                textA.append(
                    treeA.xpath(
                        f"//x:SHORT-NAME[./text() = '{id}']/..//x:L-1//text()",
                        namespaces={"x": "http://autosar.org/schema/r4.0"},
                    )
                )
                structureA.append(
                    [
                        elem.tag[32:]
                        for elem in treeA.xpath(
                            f"//x:SHORT-NAME[./text() = '{id}']/../..//*",
                            namespaces={"x": "http://autosar.org/schema/r4.0"},
                        )
                    ]
                )

            textB = []
            structureB = []
            for id in df["idsB"]:
                textB.append(
                    treeB.xpath(
                        f"//x:SHORT-NAME[./text() = '{id}']/..//x:L-1//text()",
                        namespaces={"x": "http://autosar.org/schema/r4.0"},
                    )
                )
                structureB.append(
                    [
                        elem.tag[32:]
                        for elem in treeB.xpath(
                            f"//x:SHORT-NAME[./text() = '{id}']/../..//*",
                            namespaces={"x": "http://autosar.org/schema/r4.0"},
                        )
                    ]
                )

            df["Text Release A"] = [prettifyString(x) for x in textA]
            df["Text Release B"] = [prettifyString(x) for x in textB]

            df["structureA"] = structureA
            df["structureB"] = structureB

            df["Change Rate Jaccard"] = 1 - df.apply(jaccard_similarity, axis=1)
            df["Change Rate Edit Distance"] = df.apply(lev_distance, axis=1)

            helpCol = df["type"].copy()
            df.loc[
                (df["Change Rate Edit Distance"] != 0) & (helpCol == "unchanged"),
                "type",
            ] = "changed"

            df.to_pickle(f"./data/{date}/df_{file}.pkl")
            help_list.append(df)
            print(
                f"Compared {file1} and {file2}, ({counter}/{len(all_files_year1)} documents)"
            )
            counter = counter + 1


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
