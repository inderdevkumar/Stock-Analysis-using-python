import pandas as pd
import logging as log
from nse_package import NSE_Stocks_Selection
import os
import shutil

class TestNSE:

    def __init__(self):
        # (venv) E:\inder_dont_delete>python utilities_python\share_stock_market\equity_nse\test_stock_selection_from_nse.py

        # Library methods
        nse_obj = NSE_Stocks_Selection()
        self.working_path = nse_obj.path2
        nse_obj.pre_open_market()
        nse_obj.oi_spurts()
        nse_obj.each_sectors_stocks()
        nse_obj.gainers_loosers()
        nse_obj.close()

        # ========== Fetch stocks from nse with filters ===

        self.gainers_loosers_p= 0.65
        self.gainers_loosers_lowest_cutoff= 0.1
        self.pre_open_market= 0.2
        self.sectors= 0.35
        self.gainer_loosers_highest_cutoff= 0.9

        # ========== OI To get more stocks under filter ===
        self.oi_pchange= 2

        # Move the file
        src_dir = r"E:\inder_dont_delete\utilities_python\share_stock_market\equity_nse\datas\previous_day_looser_gainers"
        # input(f"\n\nPaste previous day 'T20-GL-gainers-FOSecP' and 'T20-GL-loosers-FOSecP' files to Dir {src_dir}\n Press Enter once done")

        list_of_files= os.listdir(self.working_path)
        log.info(f"list_of_files in dir '{self.working_path}' is {list_of_files}")
        previous_day_gainers_loosers= ["T20-GL-gainers-FOSecP.csv", "T20-GL-loosers-FOSecP.csv"]
        previous_day_gainers_loosers_present= [True for item in previous_day_gainers_loosers if item in list_of_files]
        if all(previous_day_gainers_loosers_present) and bool(previous_day_gainers_loosers_present):
            log.info(f"'T20-GL-gainers-FOSecP' and 'T20-GL-loosers-FOSecP' files already present in dir '{self.working_path}'")
            log.info(f"previous_day_gainers_loosers_present bool {previous_day_gainers_loosers_present}")
        else:
            # Loop through files in source directory
            for filename in os.listdir(src_dir):
                src_file = os.path.join(src_dir, filename)
                dst_file = os.path.join(self.working_path, filename)
                # Only move files (skip subdirectories)
                if os.path.isfile(src_file):
                    shutil.copy2(src_file, dst_file)
                    log.info(f"Copied: {filename}")


    def dummy_method(self):
        r"""
        Click on sectoral indices: https://www.nseindia.com/market-data/live-market-indices/heatmap
        Click on Sector: https://www.nseindia.com/market-data/live-market-indices/heatmap

        Copy all stocks listed as it is and paste in "nse_sector_stock_list.txt" file under
        folder E:\inder_dont_delete\utilities_python\share_stock_market\notes\intraday_strategies\data

        Returns:

        """

        data_path= r"E:\inder_dont_delete\utilities_python\share_stock_market\notes\intraday_strategies\data"
        file_name = "nse_sector_stock_list.txt"
        list_of_sectors = ["NIFTY AUTO", "NIFTY BANK", "NIFTY ENERGY",
                           "NIFTY FINANCIAL SERVICES", "NIFTY FINANCIAL SERVICES 25/50", "NIFTY FMCG",
                           "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
                           "NIFTY PHARMA", "NIFTY PSU BANK", "NIFTY REALTY",
                           "NIFTY PRIVATE BANK", "NIFTY HEALTHCARE INDEX", "NIFTY CONSUMER DURABLES",
                           "NIFTY OIL & GAS", "NIFTY MIDSMALL HEALTHCARE"]



    def stocks_for_long(self):
        log.info(f"\n\n=============   stocks_for_long =========== ")
        folder_path= self.working_path
        previous_day_gainers= "T20-GL-gainers-FOSecP.csv"
        current_day_gainers= "T20-GL-gainers-FOSec.csv"
        pre_open_market_per_change= "MW-Pre-Open-Market.csv"

        all_csvlist= [previous_day_gainers, current_day_gainers, pre_open_market_per_change]
        sectors = []
        for file in os.listdir(folder_path):
            if file.startswith("MW-NIFTY"):
                sectors.append(file)
        all_csvlist.extend(sectors)


        all_datframe_after_filters = []
        for csv in all_csvlist:
            df = pd.read_csv(r"%s\%s" %(folder_path, csv))
            # Rename columns
            # df.rename(columns={'SYMBOL \n': 'Symbol', '%CHNG \n': '%chng'}, inplace=True)
            # Get only 2 columns from each Excel
            df_updated = df[['Symbol', '%chng']].copy()
            # Add a new column with excel coming from
            df_updated["From CSV"] = csv[:-4]
            # Change datatype from str to int
            df_updated['%chng'] = pd.to_numeric(df_updated['%chng'], errors='coerce')

            filtered_df1= df_updated[(df_updated['%chng'] >= self.gainers_loosers_p) & (df_updated['From CSV'] == 'T20-GL-gainers-FOSecP')]
            filtered_df2= df_updated[(df_updated['%chng'] >= self.gainers_loosers_lowest_cutoff) & (df_updated['From CSV'] == 'T20-GL-gainers-FOSec')]
            filtered_df3 = df_updated[(df_updated['%chng'] >= self.pre_open_market) & (df_updated['From CSV'] == 'MW-Pre-Open-Market')]
            filtered_df4 = df_updated[(df_updated['%chng'] >= self.sectors) & (df_updated['From CSV'].str.startswith('MW-NIFTY'))]
            all_datframe_after_filters.extend([filtered_df1, filtered_df2, filtered_df3, filtered_df4])

        # Concatenate all DataFrames in the list
        filtered_first_4_csvs_df  = pd.concat(all_datframe_after_filters, ignore_index=False)


        oi_spurts = "Spurts-in-OI-By-Underlying.csv"
        oi_spurts_df = pd.read_csv(r"%s\%s"%(folder_path, oi_spurts))
        # Get only 2 columns from each Excel
        oi_df_updated = oi_spurts_df[['Symbol', '%chng in OI']].copy()

        oi_df_updated['%chng in OI'] = pd.to_numeric(oi_df_updated['%chng in OI'], errors='coerce')
        oi_filtered_df = oi_df_updated[(oi_df_updated['%chng in OI'] >= self.oi_pchange)]


        # Step 1: Filter df1 where 'Symbol' is in df2['Symbol']
        filtered_df1 = filtered_first_4_csvs_df[filtered_first_4_csvs_df['Symbol'].isin(oi_filtered_df['Symbol'])]

        # Step 2: Filter df2 where 'Symbol' is in df1['Symbol']
        filtered_df2 = oi_filtered_df[oi_filtered_df['Symbol'].isin(filtered_first_4_csvs_df['Symbol'])]

        # Step 3: merge to see combined info
        merged_df = pd.merge(filtered_df1, filtered_df2, left_on='Symbol', right_on='Symbol')
        log.info(merged_df)
        merged_df.to_excel(r"%s\%s"%(folder_path, "long_stocks.xlsx"), index= False)



    def stocks_for_short(self):
        log.info(f"\n\n=============   stocks_for_short =========== ")
        folder_path = self.working_path
        previous_day_loosers = "T20-GL-loosers-FOSecP.csv"
        current_day_loosers = "T20-GL-loosers-FOSec.csv"
        pre_open_market_per_change = "MW-Pre-Open-Market.csv"

        all_csvlist = [previous_day_loosers, current_day_loosers, pre_open_market_per_change]
        sectors = []
        for file in os.listdir(folder_path):
            if file.startswith("MW-NIFTY"):
                # Prints only text file present in My Folder
                sectors.append(file)
        all_csvlist.extend(sectors)

        all_datframe_after_filters = []
        for csv in all_csvlist:
            df = pd.read_csv(r"%s\%s" % (folder_path, csv))
            # Rename columns
            # df.rename(columns={'SYMBOL \n': 'Symbol', '%CHNG \n': '%chng'}, inplace=True)
            # Get only 2 columns from each Excel
            df_updated = df[['Symbol', '%chng']].copy()
            # Add a new column with excel coming from
            df_updated["From CSV"] = csv[:-4]
            # Change datatype from str to int
            df_updated['%chng'] = pd.to_numeric(df_updated['%chng'], errors='coerce')

            filtered_df1 = df_updated[(df_updated['%chng'] <= -self.gainers_loosers_p) & (df_updated['From CSV'] == 'T20-GL-loosers-FOSecP')]
            filtered_df2 = df_updated[
                (df_updated['%chng'] <= -self.gainers_loosers_lowest_cutoff) & (df_updated['From CSV'] == 'T20-GL-loosers-FOSec')]
            filtered_df3 = df_updated[(df_updated['%chng'] <= -self.pre_open_market) & (df_updated['From CSV'] == 'MW-Pre-Open-Market')]
            filtered_df4 = df_updated[(df_updated['%chng'] <= -self.sectors) & (df_updated['From CSV'].str.startswith('MW-NIFTY'))]
            all_datframe_after_filters.extend([filtered_df1, filtered_df2, filtered_df3, filtered_df4])

        # Concatenate all DataFrames in the list
        filtered_first_4_csvs_df = pd.concat(all_datframe_after_filters, ignore_index=False)

        oi_spurts = "Spurts-in-OI-By-Underlying.csv"
        oi_spurts_df = pd.read_csv(r"%s\%s" % (folder_path, oi_spurts))
        # Get only 2 columns from each Excel
        oi_df_updated = oi_spurts_df[['Symbol', '%chng in OI']].copy()

        oi_df_updated['%chng in OI'] = pd.to_numeric(oi_df_updated['%chng in OI'], errors='coerce')
        oi_filtered_df = oi_df_updated[(oi_df_updated['%chng in OI'] >= self.oi_pchange)]

        # Step 1: Filter df1 where 'Symbol' is in df2['Symbol']
        filtered_df1 = filtered_first_4_csvs_df[filtered_first_4_csvs_df['Symbol'].isin(oi_filtered_df['Symbol'])]

        # Step 2: Filter df2 where 'Symbol' is in df1['Symbol']
        filtered_df2 = oi_filtered_df[oi_filtered_df['Symbol'].isin(filtered_first_4_csvs_df['Symbol'])]

        # Step 3: merge to see combined info
        merged_df = pd.merge(filtered_df1, filtered_df2, left_on='Symbol', right_on='Symbol')
        # Remove duplicate rows
        # df_cleaned = merged_df.drop_duplicates(subset=['Symbol'], keep='first')
        log.info(merged_df)
        merged_df.to_excel(r"%s\%s"%(folder_path, "short_stocks.xlsx"), index= False)

    def find_occurance_and_sort_loosers_gainers(self, df, L_S, file_mode):
        log.info(f"\n\n=============   find_occurance_and_sort_loosers_gainers =========== ")
        # Step 1: Find occurrence of column 'Symbol' (full DataFrame)
        occurrences_dict = df['Symbol'].value_counts().to_dict()
        log.info(f"{L_S} Step 1 - Occurrences of Symbol: {occurrences_dict}")

        # Step 2: Filter df with column 'y' values < 3
        if (L_S == "LONG"):
            filtered_df = df[(df['%chng'] <= self.gainer_loosers_highest_cutoff) & (df['From CSV'] == "T20-GL-gainers-FOSec")]
        else:
            filtered_df = df[(df['%chng'] >= -self.gainer_loosers_highest_cutoff) & (df['From CSV'] == "T20-GL-loosers-FOSec")]
        log.info(f"\n{L_S} Step 2 - Filtered DataFrame: {filtered_df}\n")

        # Step 3: Print only those dict entries from Step 1 that appear in filtered_df
        final_dict = {symbol: occurrences_dict[symbol] for symbol in filtered_df['Symbol'].unique()}
        log.info(f"\n{L_S}  Step 3 - Final Dictionary: {final_dict}\n")

        # Step 4: Sort final dict in descending order by value
        sorted_final_dict = dict(sorted(final_dict.items(), key=lambda item: item[1], reverse=True))
        log.info(f"For {L_S} Step 4- After sorting: {sorted_final_dict}\n")
        with open(r"%s\%s"%(self.working_path, "text_to_display.txt"), file_mode) as file:
            file.write(f"For {L_S}\n, Opperunities can be here:   {sorted_final_dict}\n\n")

    def get_min_max_per_from_fetched_stocks_excel(self, df, excel_file, file_mode):
        log.info(f"\n\n=============   get_min_max_per_from_fetched_stocks_excel =========== ")
        if excel_file.startswith("Spurts-in-OI"):
            positive_vals = df[df['%chng in OI'] > 0]['%chng in OI']
            min_val = positive_vals.min()
            max_val = positive_vals.max()
        elif excel_file.startswith("MW-NIFTY"):
            min_val = df['Min_Max'].min()
            max_val = df['Min_Max'].max()
        else:
            min_val = df['%chng'].min()
            max_val = df['%chng'].max()
        log.info(f"For file {excel_file}: min= {min_val} and max= {max_val} ")
        with open(r"%s\%s"%(self.working_path, "text_to_display.txt"), file_mode) as file:
            file.write(f"For file=> {excel_file}:   min= {min_val} and max= {max_val}\n")

    def test_oppertunity_in_long_and_short(self):
        log.info(f"\n\n=============   test_oppertunity_in_long_and_short =========== ")
        self.stocks_for_long()
        self.stocks_for_short()

        long_df = pd.read_excel(r"%s\%s"%(self.working_path, "long_stocks.xlsx"))
        self.find_occurance_and_sort_loosers_gainers(long_df, "LONG", "w")

        short_df= pd.read_excel(r"%s\%s"%(self.working_path, "short_stocks.xlsx"))
        self.find_occurance_and_sort_loosers_gainers(short_df, "SHORT", "a")

        for file in os.listdir(self.working_path):
            if file.endswith(".csv"):
                df = pd.read_csv(r"%s\%s" % (self.working_path, file))
                self.get_min_max_per_from_fetched_stocks_excel(df, file, "a")



# obj1= TestNSE()
# obj1.test_oppertunity_in_long_and_short()
