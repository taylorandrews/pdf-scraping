import tabula
import pandas as pd
import numpy as np

def get_entity_ownership(fp):
    'A | Entity Ownership'
    entity_ownership = tabula.read_pdf(fp
                                       , area = [103, 22, 113, 266]
                                       , pages = 1
                                       )

    return entity_ownership.columns[0].replace(',', '')

def get_operator(fp):
    'D | Operator'
    operator = tabula.read_pdf(fp
                               , area = [94, 280, 103, 524]
                               , pages = 1
                               )

    return operator.columns[0].replace(',', '')

def get_check_month(fp):
    'F | Check Month'
    check_month = tabula.read_pdf(fp
                                  , area = [118, 673, 127, 787]
                                  , pages = 1
                                  )

    return check_month.columns[0]

def build_header(fp):
    entity_ownership = get_entity_ownership(fp)
    operator = get_operator(fp)
    check_month = get_check_month(fp)
    header = [entity_ownership, operator, check_month]
    return header

def extract_data(fp, header, speedy=False):

    # this finds the top row on a first page:
    if speedy:
        # cheat:
        # row_search_top = 332 # 4/5 files
        row_search_top = 284 # diamondback
    else:
        cell_A1 = ''
        row_search_top = 236
        height = 15

        while 'Property:' not in cell_A1:
            df = tabula.read_pdf(fp
                            , area = [row_search_top, 22, row_search_top + height, 772]
                            , pages = 1
                            , pandas = {'header': None}
                            )
            if df is not None:
                cell_A1 = df.columns[0]
            row_search_top += 12


    first_page_data = tabula.read_pdf(fp
                        , area = [row_search_top - 12, 22, 560, 772]
                        , pages = 1
                        , pandas = {'header': None}
                        , multiple_tables = True
                        )

    other_pages_data = tabula.read_pdf(fp
                        , area = [68, 22, 560, 772]
                        , pages = 'all' # [1, 2, 3]
                        , pandas = {'header': None}
                        , multiple_tables = True
                        )[1:]

    all_data = first_page_data + other_pages_data

    months_search = ['Jan ', 'Feb ', 'Mar ', 'Apr ', 'May ', 'Jun ',
                     'Jul ', 'Aug ', 'Sep ', 'Oct ', 'Nov ', 'Dec ']

    columns = ['entity_ownership'
             , 'property_name'
             , 'state'
             , 'county'
             , 'operator'
             , 'well_name'
             , 'check_month'
             , 'production_date'
             , 'os_distribution_interest'
             , 'energy_type'
             , 'na_type'
             , 'pv_btu'
             , 'pv_volume'
             , 'pv_price'
             , 'pv_value'
             , 'os_owner_interest'
             , 'os_volume'
             , 'os_value']

    df = pd.DataFrame(columns=columns, dtype=str) # 18

    for page in all_data:
        for (idx, row_raw) in page.iterrows():
            row_raw_no_na = row_raw.dropna()
            if 'Property:' in str(row_raw[0]):
                property_info = process_property_row(row_raw)
            elif len(row_raw_no_na) == 1 and row_raw_no_na.index == [0] and 'DOI:' not in row_raw_no_na[0]:
                energy_type = process_energy_type_row(row_raw)

            if len(row_raw[row_raw.str.contains('|'.join(months_search))==True]) > 0: # row contains mmm
                idx_month = int(row_raw[row_raw.str.contains('|'.join(months_search))==True].index[0])
                row_processed = process_row(row_raw, header, property_info, energy_type, idx_month)
                row_pandas = pd.Series(data=row_processed, index=columns, dtype=str)
                df = df.append([row_pandas], ignore_index=True)

    return df

def process_energy_type_row(row):
    # TODO: process more using a set of energy_type names for more consistent output
    # ex. 'OIL' vs 'O - OIL'
    row.dropna(inplace=True)
    energy_type = row.iloc[0]
    if 'DOI:' not in energy_type:
        return row.iloc[0]

def process_property_row(row):
    row.dropna(inplace=True)
    row = row.reset_index(drop=True)
    if row[0] == 'Property:':
        property_name = [row[row != 'Property:'].iloc[0]]
        property_info = row[row != 'Property:'].iloc[1]
    else:
        property_name = [row.iloc[0][10:]]
        property_info = row.iloc[1]

    suffix_loc = property_info.find(', API: ')
    if suffix_loc > 0:
        property_info = property_info[:suffix_loc]

    state = next(word for word in property_info.split(' ')[::-1] if len(word) == 2)
    idx_state = property_info.rindex(state)
    well_name = property_info[:idx_state - 1]
    county = property_info[idx_state + 3:]
    property = [state, county, well_name]
    property_ready = property_name + property
    return property_ready

def process_row(row, header, property_info, energy_type, idx_month):
    months_search = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    production_date = row[idx_month]
    data_begins = row.first_valid_index()
    row = list(row.iloc[data_begins:])
    idx_month = idx_month - data_begins
    row = [j for i in row for j in str(i).split('  ')] # takes '  ' out of all items in <row>

    # get na_type, production_date
    if len(row[idx_month]) == 6: # if 'mmm yy' is isolated in it's own column
        if len(row[0]) > 5:
            na_type = row[0]
            production_date = row[idx_month]
            row = row[idx_month+1:]
        else:
            print('ERROR: ******* path not covered ********')
    else: # if mmm yy is concat with another column
        first_two_elements = [str(j) for i in row[0:2] for j in str(i).split(' ')]
        for elem in first_two_elements:
            if elem in months_search:
                mmm_index = first_two_elements.index(elem)
                na_type = ' '.join(first_two_elements[:mmm_index])
                production_date = ' '.join(first_two_elements[mmm_index:mmm_index+2])
                row = [i for i in first_two_elements[mmm_index+2:] if i] + row[2:]

    # get os_owner_interest, os_distribution_interest
    interest = []
    for idx, elem in enumerate(row):
        if str(elem) != 'nan' and elem[1:-8] == '.': # finds strings of format 'x.xxxxxxxx'
            interest.append(idx)
    os_owner_interest = row.pop(interest[0])
    os_distribution_interest = row.pop(interest[1]-1)

    # get os_volume, os_value
    right_of_intesest = [x for x in row[interest[0]:] if x != 'nan']
    while len(right_of_intesest) < 2:
        right_of_intesest.insert(0, '0')
    os_volume = right_of_intesest[-2]
    os_value = right_of_intesest[-1]

    # get pv_btu, pv_volume, pv_price, pv_value
    left_of_interest = [x for x in row[:interest[0]] if x != 'nan']
    while len(left_of_interest) < 4:
        left_of_interest.insert(0, '0')
    pv_btu = left_of_interest[0]
    pv_volume = left_of_interest[1]
    pv_price = left_of_interest[2]
    pv_value = left_of_interest[3]

    row_data_ready = [header[0] # entity_ownership
                    , property_info[0] # property_name
                    , property_info[1] # state
                    , property_info[2] # county
                    , header[1] # operator
                    , property_info[3] # well_name
                    , header[2] # check_month
                    , production_date
                    , os_distribution_interest
                    , energy_type
                    , na_type
                    , pv_btu
                    , pv_volume
                    , pv_price
                    , pv_value
                    , os_owner_interest
                    , os_volume
                    , os_value]

    return row_data_ready

def transform_data(df):
    numeric_cols = ['os_distribution_interest', 'pv_btu', 'pv_volume',
        'pv_price', 'pv_value', 'os_owner_interest', 'os_volume', 'os_value']
    for col in numeric_cols:
        df[col] = df[col].str.replace('(', '-').str.rstrip(')').str.replace(',', '').astype(np.float64)

        # date columns
        df['check_month'] = pd.to_datetime(df['check_month'])
        df['production_date'] = pd.to_datetime(df['production_date'], format='%b %y')

    group_by_columns = ['entity_ownership' # entity_ownership
                , 'state' # state
                , 'county' # county
                , 'operator' # operator
                , 'well_name' # well_name
                , 'check_month' # check_month
                , 'production_date' # production_date
                , 'os_distribution_interest' # os_distribution_interest
                , 'energy_type'# energy_type
                , 'pv_volume' # pv_volume
                , 'pv_price' # pv_price
                , 'os_value' # os_value
                ]

    df_agg = pd.DataFrame(columns=group_by_columns)

    df_volumes = df[df['pv_volume'] != 0] # cols grouped by volume

    for (idx, row) in df_volumes.iterrows():
        row_agg = [row['entity_ownership'] # entity_ownership
                 , row['state'] # state
                 , row['county'] # county
                 , row['operator'] # operator
                 , row['well_name'] # well_name
                 , row['check_month'] # check_month
                 , row['production_date'] # production_date
                 , row['os_distribution_interest'] # os_distribution_interest
                 , row['energy_type'] # energy_type
                 , row['pv_volume'] # pv_volume
                 , row['pv_price'] # pv_price
                 , row['os_value'] # os_value
                 ]

        row_pandas = pd.Series(data=row_agg, index=group_by_columns)
        df_agg = df_agg.append([row_pandas], ignore_index=True)

    df_agg = df_agg.set_index(df_volumes.index)

    df_data = df[df['pv_volume'] == 0]

    cols_to_add = df_data['na_type'].unique()
    df_data_long = df_data[['na_type', 'os_value']] # strips out most cols

    volume_table_indexes = pd.Series()

    # for data_idx in df_data_long.index:
    #     for i in list(range(data_idx)):
    #         if i not in df_data_long.index:
    #             idx = i
    #     volume_table_indexes = volume_table_indexes.append(pd.Series(data=[i], index=[data_idx]))

    i = df_data_long.index[0]
    j = 0
    for data_idx in df_data_long.index:
        if i == data_idx:
            volume_table_indexes = volume_table_indexes.append(pd.Series(data=[j], index=[data_idx]))
        else:
            volume_table_indexes = volume_table_indexes.append(pd.Series(data=[i], index=[data_idx]))
            j = i
            i = data_idx
        i += 1

    value_col_df = pd.DataFrame(data={'volume_table_index': volume_table_indexes})
    df_values = df_data_long.join(value_col_df)

    # print(df_values)

    dfs_to_add = []
    for col in cols_to_add:
        os_value_series = df_values[df_values['na_type'] == col]['os_value']
        volume_table_index_series = df_values[df_values['na_type'] == col]['volume_table_index']
        df_to_add = pd.DataFrame(data={col.lower().replace(' ', '_'): os_value_series}).set_index(volume_table_index_series)
        dfs_to_add.append(df_to_add)
    # print(f'{len(dfs_to_add)}: {dfs_to_add}')
    for df in dfs_to_add:
        df_agg = df_agg.join(df, how='left')
    df_done = df_agg.fillna(0)

    return df_done

def check_extraction(df):
    # this checks a df for any columns that have a space
    cols_without_space = ['property_name', 'state', 'county',
        'os_distribution_interest', 'pv_btu', 'pv_volume', 'pv_price',
        'pv_value', 'os_owner_interest', 'os_volume', 'os_value']
    for col in cols_without_space:
        row_idx_to_drop = df[df[col].astype(str).str.contains(' ')].index
        if row_idx_to_drop.size > 0:
            df = df.drop(row_idx_to_drop, axis=0)
    return df

def process_pdf(fp, speedy):
    header = build_header(fp)
    df = extract_data(fp, header, speedy)
    df = check_extraction(df)
    df = transform_data(df)
    return df

if __name__ == '__main__':
    fps = ['DELRIO Bonanza Creek.pdf'
            , 'LEP3 Chesapeake.pdf'
            , 'DELRIO Extraction.pdf'
            , 'LEP3 Bison.pdf']
    #         , 'LEP3 Diamondback.pdf']
    # fps = ['DELRIO Bonanza Creek.pdf']
    # fps = ['LEP3 Chesapeake.pdf']

    data = '../samples/'
    output = '../results/'

    # speedy = True

    for fp in fps:
        print('processing {} ...'.format(fp))
        df = process_pdf(data + fp, speedy=False)
        df.to_csv(path_or_buf=output + fp[:-4] + '.csv', index=False)


        # TODO: put a try catch aronud this whole thing?
        # TODO: overlapping file names? will overwrite
        # TODO: nulls to 0's - entire df
        # TODO: check_extraction function improvements?
        # TODO: multiple pdfs into 1 .csv
