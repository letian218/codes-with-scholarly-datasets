# -*- coding: utf-8 -*-
"""
# Code for getting the wanted data (disruptiveness indicator) of a WoS dataset (tsv format)
@author: Letian
"""

import csv
import os
import pickle
import pandas as pd
import tqdm


def save_as(var, path, name):
    save_path = os.path.join(path, name)
    f = open(save_path, 'wb')
    pickle.dump(var, f)
    f.close()
    return


def ut2py_list(pmid_set, pmid_dict):
    py_list = []
    for pmid in pmid_set:
        py_list.append(pmid_dict[pmid])
    return pd.Series(py_list, dtype='float64')


def loop_read_csv(file_path, sep=',', usecols=None, dtype=None):
    reader = pd.read_csv(file_path, iterator=True, sep=sep, usecols=usecols, dtype=dtype)
    loop = True
    flag = 0
    chunksize = 100000
    chunks = []
    while loop:
        try:
            flag += 1
            chunk = reader.get_chunk(chunksize)
            chunks.append(chunk)
            if flag % 10 == 0:
                print('%d lines have been processed.\n' % (flag * chunksize))
        except StopIteration:
            try:
                for i in range(0, 100):
                    chunk = reader.get_chunk(1000)
                    chunks.append(chunk)
                print('Exception of RAM not enough happened!')
                if flag % 10 == 0:
                    print('%d lines have been processed.\n' % (flag * chunksize))
            except StopIteration:
                loop = False
                print('Iteration is stopped.\n')
    return pd.concat(chunks, ignore_index=True)


def disrupt_cal(df, window_list, ye):
    l = len(df)
    pmid_ind = {}
    pmid_py = {}
    for i in range(l):
        pmid = df['PMID'][i]
        pmid_ind[pmid] = i
        pmid_py[pmid] = int(df['PY'][i])
    for window in window_list:  # PREL: PRELUDE. DISRUPT = (SOLO-DUET)/(SOLO+PREL+DUET).
        df['SOLO' + str(window)] = [float('nan')] * l
        df['PREL' + str(window)] = [float('nan')] * l
        df['DUET' + str(window)] = [float('nan')] * l
    for i in tqdm.tqdm(range(l), desc='Calculate disruptiveness'):
        set1 = set(df['LC'][i])
        set2 = set()
        set3 = set()
        for pmid in df['LR'][i]:
            set2.update(df['LC'][pmid_ind[pmid]])
        set3 = set1 & set2
        set1 = ut2py_list(set1 - set3, pmid_py)
        set2 = ut2py_list(set2 - set3, pmid_py)
        set3 = ut2py_list(set3, pmid_py)
        for window in window_list:
            py_thres = df['PY'][i] + window
            if py_thres > ye:
                continue
            df.at[i, 'SOLO' + str(window)] = sum(set1 <= py_thres)
            df.at[i, 'PREL' + str(window)] = sum(set2 <= py_thres)
            df.at[i, 'DUET' + str(window)] = sum(set3 <= py_thres)
    return df


if __name__ == '__main__':
    csv.field_size_limit(500 * 1024 * 1024)

    root_path = ''  # file containing all files
    file_path_pd = ''
    file_path_cnet = ''
    ye = 2019
    window_list = [5, 60]

    df_cnet = loop_read_csv(file_path_cnet, sep='\t', usecols=['Citing_PaperID', 'Cited_PaperID'],
                            dtype={'Citing_PaperID': str, 'Cited_PaperID': str})

    file_dataframe = loop_read_csv(file_path_pd, sep='\t', usecols=['PaperID', 'DocType', 'Year', 'JournalID', 'CitationCount'],
                                   dtype={'PaperID': str, 'Year': str})
    # data check
    pmid_set = set(file_dataframe['PaperID'])
    if len(pmid_set) != len(file_dataframe):
        raise ValueError('Duplicate PMID!')
    crid_set = set(df_cnet['Citing_PaperID']) | set(df_cnet['Cited_PaperID'])
    crid_not_in_pmid_set = crid_set - pmid_set
    if len(crid_not_in_pmid_set):
        raise ValueError('Some citing/cited ids are not in pmid set!')

    # clear data of PY NAN or >2019
    cflag = []
    for i in tqdm.tqdm(range(len(file_dataframe)), desc='Clear data by year'):
        year = file_dataframe['Year'][i]
        if str(year) == 'nan':
            cflag.append(i)
        else:
            if float(year) > ye:
                cflag.append(i)
    cpmid_set = set(file_dataframe['PaperID'][cflag])

    file_dataframe = file_dataframe.drop(labels=cflag).reset_index(drop=True)
    l1 = len(file_dataframe)
    file_dataframe['PY'] = [0] * l1
    for i in tqdm.tqdm(range(l1), desc='Convert str to int'):
        file_dataframe.at[i, 'PY'] = int(float(file_dataframe['Year'][i]))
    file_dataframe.drop(['Year'], axis=1, inplace=True)
    file_dataframe.rename(columns={'PaperID': 'PMID'}, inplace=True)

    ut_ind = {}
    for i in range(l1):
        ut_ind[file_dataframe['PMID'][i]] = i
    file_dataframe['LR'] = [[]] * l1
    file_dataframe['LC'] = [[]] * l1
    for i in tqdm.tqdm(range(len(df_cnet)), desc='LR and LC generate'):
        citing_id = df_cnet['Citing_PaperID'][i]
        cited_id = df_cnet['Cited_PaperID'][i]
        if citing_id in cpmid_set or cited_id in cpmid_set:
            continue
        file_dataframe.at[ut_ind[citing_id], 'LR'] = file_dataframe.at[ut_ind[citing_id], 'LR'] + [cited_id]
        file_dataframe.at[ut_ind[cited_id], 'LC'] = file_dataframe.at[ut_ind[cited_id], 'LC'] + [citing_id]

    df = disrupt_cal(file_dataframe, window_list, ye)
    save_as(df, root_path, 'file_solo&prel&duet.data')










