import pandas as pd
import os
from lxml import etree
from Levenshtein import distance as lev
from datetime import datetime
from typing import List, Tuple


def prettifyString(string):
    return (
        " ".join(string)
        .replace("\n", " ")
        .replace("                                                          ", " ")
        .replace("    ", " ")
    )


def lev_distance(row):
    if row[6] == "" and row[7] == "":
        return 0
    return lev(row[6], row[7]) / (max(len(row[6]), len(row[7])))


def jaccard_similarity(row):
    if row[6] == "" and row[7] == "":
        return 0
    # convert to set
    a = set(row[6])
    b = set(row[7])
    # calucate jaccard similarity
    j = float(len(a.intersection(b))) / len(a.union(b))
    return j


xml_namespace = {"x": "http://autosar.org/schema/r4.0"}


def check_if_subfolders_identical(year1_folder, year2_folder):
    """This function checks wether the same subfolders are in the topfolder of year1 and the topfolder of year2"""
    all_files_year1 = os.listdir(year1_folder)
    all_files_year2 = os.listdir(year2_folder)
    if not all_files_year1 == all_files_year2:
        raise ValueError(
            f"FolderList not the same for {year1_folder} and {year2_folder}"
        )


def check_if_file_exists_for_next_year(file, year2_folder, subfolder):
    """This function checks wether a file for a certain year does exist for the next year"""
    if file not in os.listdir(f"{year2_folder}/{subfolder}/zz_generated"):
        raise ValueError(f"{file} not in {year2_folder}/{subfolder}/zz_generated")


def create_new_dir_to_save_data():
    """This fucntion creates a new directory named after the current date and time and returns the
    path to this directory"""
    path_to_dir = f"./data/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    os.mkdir(path_to_dir)
    return path_to_dir


def get_text_and_structure(id_list, xml_tree):
    texts = []
    structures = []

    for id in id_list:
        texts.append(
            prettifyString(
                xml_tree.xpath(
                    f"//x:SHORT-NAME[./text() = '{id}']/..//x:L-1//text()",
                    namespaces=xml_namespace,
                )
            )
        )
        structures.append(
            [
                elem.tag[32:]
                for elem in xml_tree.xpath(
                    f"//x:SHORT-NAME[./text() = '{id}']/../..//*",
                    namespaces=xml_namespace,
                )
            ]
        )
    return texts, structures


def compare_years(year1: str, year2: str, df_list: Tuple, path_to_folder: str):
    """This function dientifies the changes for two years and writes them as a DF to a list"""
    year1_folder = f"{path_to_folder}/{year1}"
    year2_folder = f"{path_to_folder}/{year2}"

    check_if_subfolders_identical(year1_folder, year2_folder)
    path_to_dir = create_new_dir_to_save_data()
    for subfolder in os.listdir(year1_folder):
        for document in os.listdir(f"{year1_folder}/{subfolder}/zz_generated"):
            check_if_file_exists_for_next_year(document, year2_folder, subfolder)

            # Get XML trees of entire document
            xml_tree_year1 = etree.parse(
                f"{year1_folder}/{subfolder}/zz_generated/{document}"
            )
            xml_tree_year2 = etree.parse(
                f"{year2_folder}/{subfolder}/zz_generated/{document}"
            )

            # Search for all ids in document
            xml_search_ids_string = (
                f"//*[self::x:TRACE or self::x:TRACEABLE-TABLE]/x:SHORT-NAME/text()"
            )

            ids_year1 = xml_tree_year1.xpath(
                xml_search_ids_string, namespaces=xml_namespace
            )
            ids_year2 = xml_tree_year2.xpath(
                xml_search_ids_string, namespaces=xml_namespace
            )

            # Merge together all the ids into one DF (with NAs)
            df = pd.merge(
                pd.DataFrame({"ids_year1": ids_year1}),
                pd.DataFrame({"ids_year2": ids_year2}),
                how="outer",
                left_on="ids_year1",
                right_on="ids_year2",
            )

            # Add years and document to DF
            df["previous_year"] = year1
            df["current_year"] = year2
            df["document_name"] = document

            # Add change types to DF
            df["type"] = "unchanged"
            df["type"][df.ids_year2.isna()] = "deleted"
            df["type"][df.ids_year1.isna()] = "new"

            # get entire structure and text for all ids
            year1_texts, year1_structures = get_text_and_structure(
                df["ids_year1"], xml_tree_year1
            )
            year2_texts, year2_structures = get_text_and_structure(
                df["ids_year2"], xml_tree_year2
            )
            df["text_year1"] = year1_texts
            df["text_year2"] = year2_texts
            df["structure_year1"] = year1_structures
            df["structure_year2"] = year2_structures

            # Apply Jaccard and Levenstein Method for String comparison
            df["Change Rate Jaccard"] = 1 - df.apply(jaccard_similarity, axis=1)
            df["Change Rate Edit Distance"] = df.apply(lev_distance, axis=1)

            df.loc[
                (df["Change Rate Edit Distance"] != 0) & (df["type"] == "unchanged"),
                "type",
            ] = "changed"

            # Save DFs
            df.to_pickle(f"{path_to_dir}/df_{document}.pkl")
            df_list.append(df)
            print(
                f"Compared {year1_folder}/{subfolder}/zz_generated/{document} and {year2_folder}/{subfolder}/zz_generated/{document}"
            )


def create_change_df(years: List[Tuple[str, str]], path_to_years_folder: str):
    """This function receives a List of year tuples and the path to folder in which the year directories can be founds,
    compares the files in those directories and writes the results into a DF"""
    df_list = []

    for year_tuple in years:
        compare_years(year_tuple[0], year_tuple[1], df_list, path_to_years_folder)

    return pd.concat(df_list)
