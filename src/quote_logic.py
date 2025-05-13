import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from io import StringIO

def open_html(doc_path):
    with open(doc_path, "r", encoding="utf-8") as file:
        html_quote = file.read()

    return html_quote

def df_formating(html_string_io):
    df = pd.read_html(html_string_io)[0]

    columns_ = df.iloc[2]
    columns_.dropna(inplace=True)
    columns_.rename('', inplace=True)

    df.drop(2, axis=1, inplace=True)
    df.drop([0,1,2], axis=0, inplace=True)

    df.columns = columns_
    df.dropna(inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

def df_from_html(html_quote):
    html_string = BeautifulSoup(html_quote, 'lxml')

    prod_name = html_string.select('div.skuDetails span.global_gray_bold')[0].text

    html_string = html_string.find(class_="skuDetails")
    html_string = str(html_string)
    html_string_io = StringIO(html_string)
    

    df = df_formating(html_string_io)

    return (prod_name, df)

def set_index_ (df):
    df_cp = df.copy()

    df_cp.set_index(df_cp.SKU, inplace=True)
    df_cp.drop(['SKU', 'Unit Price', 'Subtotal'], axis=1, inplace=True)

    return df_cp

def merge_clean_up (df, suffix=None, merge_type=None):
    df_cp = df.copy()

    if merge_type == 'both':
        df_cp.drop(["Description_x", "Quantity_x"], axis=1, inplace=True)
        df_cp.rename(columns={"Description_y":"Description", "Quantity_y":"Quantity"}, inplace=True)
        return df_cp

    df_cp.dropna(axis=1, inplace=True)
    df_cp.rename(columns={"Description" + suffix:"Description", "Quantity" + suffix:"Quantity"}, inplace=True)

    return df_cp

def drop_merge_column(df):
    return df.drop(["_merge"], axis=1)


if __name__ == '__main__':

    #test html document paths. 
    #long term goal is to have these items ingested from drag and drop in html page
    doc_path_1 = r"C:\Users\Zach_Schulz-Behrend\dev\quote-compare\backup_data\3000186319481.5.html"
    doc_path_2 = r"C:\Users\Zach_Schulz-Behrend\dev\quote-compare\backup_data\3000189092877.1.html"

    quote_1 = open_html(doc_path_1)
    quote_2 = open_html(doc_path_2)

    quote_1 = df_from_html(quote_1)
    quote_2 = df_from_html(quote_2)

    quote_1 = set_index_(quote_1[1])
    quote_2 = set_index_(quote_2[1])

    df_merge = quote_1.merge(quote_2, how='outer', left_index=True, right_index=True, indicator=True)

    df_BOTH = df_merge[df_merge['_merge']=="both"]
    df_X = df_merge[df_merge['_merge']=="left_only"]
    df_Y = df_merge[df_merge['_merge']=="right_only"]
    
    quote_2_unique = merge_clean_up(df_Y, suffix='_y')
    quote_2_unique = drop_merge_column(quote_2_unique)

    quote_1_unique = merge_clean_up(df_X, suffix='_x')
    quote_1_unique = drop_merge_column(quote_1_unique) 

    quote_common_items = merge_clean_up(df_BOTH, merge_type='both')
    quote_common_items = drop_merge_column(quote_common_items)

    print(quote_1_unique)
    print('\n')
    print(quote_2_unique)
    print('\n')
    print(quote_common_items)