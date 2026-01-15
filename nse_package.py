import requests
import pandas as pd
import time
import logging as log
from datetime import datetime
import os

class NSE_Stocks_Selection:
    def __init__(self):
        self.sectoral_p= 0.45
        self.delay = 2
        working_dir = r"E:\inder_dont_delete\utilities_python\share_stock_market\equity_nse\datas"

        # Create both parent and child folders if not present
        child_folder_name = datetime.today().strftime("%d_%b").lower()
        parent = datetime.today().strftime("%b_%Y").lower()
        path1 = os.path.join(working_dir, parent)

        self.path2 = os.path.join(path1, child_folder_name)
        os.makedirs(self.path2, exist_ok=True)

        # Configure logging
        log.basicConfig(filename=f'%s\intraday_logs.log' % (self.path2),
                        filemode="w", format="%(asctime)s - %(levelname)s - %(message)s",
                        level=log.DEBUG)
        log.info(f"\n\n============ __init__ ==========")
        log.info(f"Created folder: {self.path2}")

        # Create one session for the whole client
        self.session = requests.Session()

    def retry_and_handle_exception(self, url_to_featch, nse_headers, retries=3):
        for attempt in range(1, retries + 1):
            try:
                # response = requests.get(url, timeout=5)
                response = self.session.get(url=url_to_featch, headers=nse_headers)
                log.info(f"Attempt {attempt}: Status {response.status_code}")

                if response.status_code == 200:
                    return response
                else:
                    time.sleep(self.delay)
            except requests.exceptions.RequestException as e:
                log.info(f"Attempt {attempt}: Error - {e}")
                time.sleep(self.delay)

        log.info("Failed after retries.")
        return None


    def pre_open_market(self):
        log.info("\n\n================ pre_open_market ===========")
        url_to_featch = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            "Accept-Language": "en-US,en;q=0.9"}
        # nse_headers= {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        #               "Accept-Language": "en-US,en;q=0.9",
        #               "Cookie": "copy paste"}
        pre_open_market_data = self.retry_and_handle_exception(url_to_featch, nse_headers)

        try:
            pre_open_market_json = pre_open_market_data.json()
            df = pd.json_normalize(pre_open_market_json["data"])
            df_required_columns = df[["metadata.symbol", "detail.preOpenMarket.perChange"]].copy()
            # Rename columns
            df_required_columns.rename(columns={'metadata.symbol': 'Symbol', 'detail.preOpenMarket.perChange': '%chng'}, inplace=True)
            log.info(df_required_columns.columns)
            log.info(df_required_columns.head())
            df_required_columns.to_csv(f'%s\MW-Pre-Open-Market.csv' % (self.path2), index=False)
            time.sleep(self.delay)
            # for item in pre_open_market_json["data"]:
            #     print(item["metadata"], len(item["metadata"]))
            #     print(f'{item["metadata"]["symbol"]} {item["metadata"]["pChange"]}')

        except Exception as e:
            # Catch any exception and print/log it
            log.info(f"An error occurred: {e}")
            print(f"An error occurred in pre_open_market: {e}")

    def top_20_gainers(self):
        log.info("\n\n================ top_20_gainers ===========")
        url_to_featch = "https://www.nseindia.com/api/live-analysis-variations?index=gainers"
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            "Accept-Language": "en-US,en;q=0.9"}
        top_gainers_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
        try:
            top_gainers_json = top_gainers_data.json()
            df = pd.json_normalize(top_gainers_json["FOSec"]["data"])
            df_required_columns = df[["symbol", "perChange"]].copy()
            df_required_columns.rename(columns={'symbol': 'Symbol', 'perChange': '%chng'}, inplace=True)
            # Rename columns
            log.info(df_required_columns.columns)
            log.info(df_required_columns.head())
            df_required_columns.to_csv(f'%s\T20-GL-gainers-FOSec.csv' % (self.path2), index=False)
            time.sleep(self.delay)
        except Exception as e:
            # Catch any exception and print/log it
            log.info(f"An error occurred: {e}")
            print(f"An error occurred in top_20_gainers: {e}")

    def top_20_loosers(self):
        log.info("\n\n================ top_20_loosers ===========")
        url_to_featch = "https://www.nseindia.com/api/live-analysis-variations?index=loosers"
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            "Accept-Language": "en-US,en;q=0.9"}

        top_loosers_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
        try:
            top_loosers_json = top_loosers_data.json()
            df = pd.json_normalize(top_loosers_json["FOSec"]["data"])
            df_required_columns = df[["symbol", "perChange"]].copy()
            # Rename columns
            df_required_columns.rename(columns={'symbol': 'Symbol', 'perChange': '%chng'}, inplace=True)
            log.info(df_required_columns.columns)
            log.info(df_required_columns.head())
            df_required_columns.to_csv(f'%s\T20-GL-loosers-FOSec.csv' % (self.path2), index=False)
            time.sleep(self.delay)
        except Exception as e:
            # Catch any exception and print/log it
            log.info(f"An error occurred: {e}")
            print(f"An error occurred in top_20_loosers: {e}")

    def gainers_loosers(self):
        log.info("\n\n================ gainers_loosers ===========")
        url_to_featch = "https://www.nseindia.com/api/live-analysis-stocksTraded"
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            "Accept-Language": "en-US,en;q=0.9"}

        top_loosers_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
        try:
            top_loosers_json = top_loosers_data.json()
            df = pd.json_normalize(top_loosers_json["total"]["data"])
            df_required_columns = df[["symbol", "pchange"]].copy()
            log.info(df_required_columns.columns)
            # Rename columns
            df_required_columns.rename(columns={'symbol': 'Symbol', 'pchange': '%chng'}, inplace=True)
            log.info(df_required_columns.columns)
            log.info(df_required_columns.head())

            pre_open_df= pd.read_csv(f'%s\MW-Pre-Open-Market.csv' % (self.path2))
            fo_stocks_list= pre_open_df["Symbol"].tolist()

            # Filter rows where 'Symbol' column values exist in fo_stocks_list
            # For gainers
            filtered_fo_stocks_gainer_df = df_required_columns[df_required_columns["Symbol"].isin(fo_stocks_list) & (df_required_columns["%chng"] >= 0.35)]
            filtered_fo_stocks_gainer_df.to_csv(f'%s\T20-GL-gainers-FOSec.csv' % (self.path2), index=False)

            # For looser
            filtered_fo_stocks_loosers_df = df_required_columns[
                df_required_columns["Symbol"].isin(fo_stocks_list) & (df_required_columns["%chng"] <= -0.35)]
            filtered_fo_stocks_loosers_df.to_csv(f'%s\T20-GL-loosers-FOSec.csv' % (self.path2), index=False)
            time.sleep(self.delay)
        except Exception as e:
            # Catch any exception and print/log it
            log.info(f"An error occurred: {e}")
            print(f"An error occurred in gainers_loosers: {e}")

    def oi_spurts(self):
        log.info("\n\n================ oi_spurts ===========")
        url_to_featch = "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings"
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            "Accept-Language": "en-US,en;q=0.9"}

        top_loosers_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
        try:
            top_loosers_json = top_loosers_data.json()
            df = pd.json_normalize(top_loosers_json["data"])
            df_required_columns = df[["symbol", "avgInOI"]].copy()
            # Rename columns
            df_required_columns.rename(columns={'symbol': 'Symbol', 'avgInOI': '%chng in OI'}, inplace=True)
            log.info(df_required_columns.columns)
            log.info(df_required_columns.head())
            df_required_columns.to_csv(f'%s\Spurts-in-OI-By-Underlying.csv' % (self.path2), index=False)
            time.sleep(self.delay)
        except Exception as e:
            # Catch any exception and print/log it
            log.info(f"An error occurred: {e}")
            print(f"An error occurred in oi_spurts: {e}")


    def __sectors_heatmap(self):
        log.info("\n\n================ sectors_heatmap ===========")
        url_to_featch = "https://www.nseindia.com/api/heatmap-index?type=Sectoral%20Indices"
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
            "Accept-Language": "en-US,en;q=0.9"}

        sectors_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
        try:
            sectors_json = sectors_data.json()
            df = pd.json_normalize(sectors_json)
            df_required_columns = df[["index", "pChange"]].copy()
            # Rename columns
            log.info(df_required_columns.columns)
            log.info(df_required_columns.head())

            filtered_for_long_df = df_required_columns[df_required_columns["pChange"] >= self.sectoral_p]
            if filtered_for_long_df.empty:
                sectors_long_dict= {}
            else:
                sectors_long_dict= dict(zip(filtered_for_long_df.iloc[:, 0], filtered_for_long_df.iloc[:, 1]))

            filtered_for_short_df = df_required_columns[df_required_columns["pChange"] <= -self.sectoral_p]
            if filtered_for_short_df.empty:
                sectors_short_dict= {}
            else:
                sectors_short_dict = dict(zip(filtered_for_short_df.iloc[:, 0], filtered_for_short_df.iloc[:, 1]))

            # sectors_long = filtered_for_long_df["index"].values
            # sectors_short = filtered_for_short_df["index"].values

            log.info(f"Sectors for long: {sectors_long_dict}, for short: {sectors_short_dict}")
            time.sleep(self.delay)
            return [sectors_long_dict, sectors_short_dict]
        except Exception as e:
            # Catch any exception and print/log it
            log.info(f"An error occurred: {e}")
            print(f"An error occurred in sectors_heatmap: {e}")


    def each_sectors_stocks(self):
        log.info("\n\n================ each_sectors_stocks ===========")
        sectors_long_short = self.__sectors_heatmap()
        sectors_long = sectors_long_short[0]
        sectors_short = sectors_long_short[1]
        sectoral_url = {
            "NIFTY AUTO": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20AUTO",
            "NIFTY BANK": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20BANK",
            "NIFTY FIN SERVICE": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20FIN%20SERVICE",
            "NIFTY FINSRV25 50": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20FINSRV25%2050",
            "NIFTY FMCG": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20FMCG",
            "NIFTY IT": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20IT",
            "NIFTY MEDIA": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20MEDIA",
            "NIFTY METAL": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20METAL",
            "NIFTY PHARMA": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20PHARMA",
            "NIFTY PSU BANK": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20PSU%20BANK",
            "NIFTY REALTY": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20REALTY",
            "NIFTY PVT BANK": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20PVT%20BANK",
            "NIFTY HEALTHCARE": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20HEALTHCARE",
            "NIFTY CONSR DURBL": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20CONSR%20DURBL",
            "NIFTY OIL AND GAS": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20OIL%20AND%20GAS",
            "NIFTY MIDSML HLTH": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20MIDSML%20HLTH",
            "NIFTY CHEMICALS": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20CHEMICALS",
            "NIFTY500 HEALTH": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY500%20HEALTH",
            "NIFTY FINSEREXBNK": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20FINSEREXBNK",
            "NIFTY MS FIN SERV": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20MS%20FIN%20SERV",
            "NIFTY MS IT TELCM": "https://www.nseindia.com/api/heatmap-symbols?type=Sectoral%20Indices&indices=NIFTY%20MS%20IT%20TELCM",
        }

        for key, value in sectoral_url.items():
            if key in sectors_long.keys():
                url_to_featch = value
                nse_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
                    "Accept-Language": "en-US,en;q=0.9"}
                sectors_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
                try:
                    sectors_json = sectors_data.json()
                    df = pd.json_normalize(sectors_json)
                    df_required_columns = df[["symbol", "pChange"]].copy()
                    df_required_columns["seectoralP"] = sectors_long[key]  # Replace with Sectoral pChange, as we already have stock pChange from gainetrs/lossers
                    # Rename columns
                    df_required_columns.rename(columns={'symbol': 'Symbol', 'pChange': 'Min_Max', 'seectoralP': '%chng'}, inplace=True)
                    log.info(df_required_columns.head())
                    df_required_columns.to_csv(f'%s\MW-NIFTY-LG-%s.csv' % (self.path2, key.replace(" ", "_")), index=False)
                except Exception as e:
                    # Catch any exception and print/log it
                    log.info(f"An error occurred: {e}")
                    print(f"An error occurred in each_sectors_stocks {key}: {e}")
            elif key in sectors_short:
                url_to_featch = value
                nse_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
                    "Accept-Language": "en-US,en;q=0.9"}
                sectors_data = self.retry_and_handle_exception(url_to_featch, nse_headers)
                try:
                    sectors_json = sectors_data.json()
                    df = pd.json_normalize(sectors_json)
                    df_required_columns = df[["symbol", "pChange"]].copy()
                    df_required_columns["seectoralP"] = sectors_short[key]
                    # Rename columns
                    df_required_columns.rename(columns={'symbol': 'Symbol', 'pChange': 'Min_Max', 'seectoralP': '%chng'}, inplace=True)
                    log.info(df_required_columns.head())
                    df_required_columns.to_csv(f'%s\MW-NIFTY-ST-%s.csv' % (self.path2, key.replace(" ", "_")), index=False)
                except Exception as e:
                    # Catch any exception and print/log it
                    log.info(f"An error occurred: {e}")
                    print(f"An error occurred in each_sectors_stocks {key}: {e}")
            else:
                pass
            time.sleep(self.delay)

    def close(self):
        self.session.close()