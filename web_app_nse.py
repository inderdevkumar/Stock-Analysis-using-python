import streamlit as st
import pandas as pd
from test_stock_selection_from_nse import TestNSE
from datetime import datetime

# Command: (venv) E:\inder_dont_delete>streamlit run utilities_python\share_stock_market\equity_nse\web_app_nse.py
# Open in edge: Local URL: http://localhost:8501

class TestWebApp:
    def __init__(self):
        # --- Streamlit UI ---
        st.set_page_config(layout="wide")
        st.title("Fetch Real Time stocks")

        # Inject CSS to style the button
        st.markdown(
            """
            <style>
            div.stButton > button:first-child {
                background-color: #0066ff; /* Blue */
                color: white;
                border-radius: 8px;
                height: 3em;
                width: 20em;
                font-size: 16px;
                font-weight: bold;
            }
            div.stButton > button:hover {
                background-color: #45a049; /* Darker green on hover */
                color: white;
            }
            </style>
            """,
            unsafe_allow_html=True
        )



    def day_suffix(self, day: int) -> str:
        if 11 <= day <= 13:
            return f"{day}th"
        elif day % 10 == 1:
            return f"{day}st"
        elif day % 10 == 2:
            return f"{day}nd"
        elif day % 10 == 3:
            return f"{day}rd"
        else:
            return f"{day}th"

    def test_web_app(self):
        # Button to trigger Excel creation
        if st.button("Generate Latest Dataframe"):
            obj = TestNSE()
            obj.test_oppertunity_in_long_and_short()
            stock_path = obj.working_path
            # file_path = create_excel_file()
            file1= r"%s\%s"%(stock_path, "long_stocks.xlsx")
            file2= r"%s\%s" % (stock_path, "short_stocks.xlsx")
            log_file= r"%s\%s" % (stock_path, "text_to_display.txt")

            # Get current datetime
            now = datetime.now()
            # Format date and time
            day = self.day_suffix(now.day)
            month = now.strftime("%b")  # Jan, Feb, etc.
            year = now.strftime("%Y")
            time_str = now.strftime("%I:%M %p")  # 12-hour format with AM/PM
            formatted = f"{day} {month} {year}    {time_str}"

            st.success(f"files created successfully!  ⏱ {formatted}")

            # =================
            with open(log_file, "r") as file:
                content= file.read()
            st.text(f"{content}")

            # ===================
            # Create 3 columns: left, spacer, right
            col1, spacer, col2 = st.columns([3, 1, 3])  # ratio gives space in middle

            # Display first Excel in left column
            with col1:
                st.write("### LONG")
                df1 = pd.read_excel(file1)
                st.dataframe(df1, width='stretch')

            # Spacer column (empty)
            with spacer:
                st.write(" ")

            # Display second Excel in right column
            with col2:
                st.write("### SHORT")
                df2 = pd.read_excel(file2)
                st.dataframe(df2, width='stretch')

st_obj= TestWebApp()
st_obj.test_web_app()