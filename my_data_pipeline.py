from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
import pandas as pd
import sqlite3
import requests
import csv

default_args = {
    'owner': 'Harsh Agrawal',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def scrape_data():
    website = 'https://himkosh.nic.in/eHPOLTIS/PublicReports/wfrmBudgetAllocationbyFD.aspx'
    path = r"C:/Users/DELL/Downloads/chromedriver_win32/chromedriver.exe"
    #web scraping 
    driver = webdriver.Chrome(path)
    driver.get(website)

    #selecting input date from calander
    from_date = driver.find_element(By.ID, 'txtFromDate')
    from_date.click()                      
    from_date.send_keys(Keys.CONTROL, "a") 
    from_date.send_keys(Keys.BACKSPACE)    
    from_date.send_keys("01/04/2018") 
    from_date.click()

    #click on Done button
    button = driver.find_element_by_xpath('//button[@data-handler = "hide"]')
    button.click()

    #selecting input date from calander
    to_date = driver.find_element(By.ID, 'txtQueryDate')
    to_date.click()                      
    to_date.send_keys(Keys.CONTROL, "a") 
    to_date.send_keys(Keys.BACKSPACE)    
    to_date.send_keys("31/03/2022")

    #click on Done button
    button = driver.find_element_by_xpath('//button[@data-handler = "hide"]')
    button.click()

    #select report type
    report_type = Select(driver.find_element(By.ID, 'ddlQuery'))
    report_type.select_by_visible_text('Demand and HOA Wise Summary')

    #selecting Rupees on radio button
    radio = driver.find_element(By.ID, 'MainContent_rbtUnit_0')   
    radio.send_keys("Rupees") 
    radio.click()

    #clicking on View Data button
    button = driver.find_element_by_xpath('//input[@value="View Data"]')
    button.click()

    #fetching the data using tagid and tag name
    table_element = driver.find_element(By.ID, 'hodAllocation')
    table_rows = table_element.find_elements(By.TAG_NAME, 'tr')
    
    #store data in a CSV file
    csv_file = open('table_data.csv', 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)

    #writing data to csv file
    for row in table_rows:
        #extract the table cells for each row
        cells = row.find_elements(By.TAG_NAME, 'td')
        row_data = []
        for cell in cells:
            #Extract the text from each cell
            cell_text = cell.text
            row_data.append(cell_text)

        #write the row data to the CSV file
        csv_writer.writerow(row_data)

        csv_file.close() #closing csv file
        driver.quit() #quitting the driver

def transform_data():
    #Read data from the CSV file
    df = pd.read_csv(r"C:/Users/DELL/Desktop/Web Scrapping/table_data.csv",sep=',',header=2)
    
    #replacing NaN with some value
    clean_df1 = df.fillna('01-VIDHANSABHA')

    #filtering Total from DmdCd and HOA
    clean_df1 = clean_df1[ (df['DmdCd'] != 'Grand Total') & (df['HOA'] != 'Total')]

    #splitting DmdCd
    splitted_df = clean_df1["DmdCd"].str.split("-", n = 1, expand = True)  
    clean_df1["DemandCode"]= splitted_df[0]
    clean_df1["Demand"]= splitted_df[1]

    #splitting HOA column values
    splitted_df = clean_df1["HOA"].apply(lambda x: pd.Series(str(x).split("-"))) 
    clean_df1["MajorHead"]= splitted_df[0]
    clean_df1["SubMajorHead"]= splitted_df[1]
    clean_df1["MinorHead"]= splitted_df[2]
    clean_df1["SubMinorHead"]= splitted_df[3]
    clean_df1["DetailHead"]= splitted_df[4]
    clean_df1["SubDetailHead"]= splitted_df[5]
    clean_df1["BudgetHead"]= splitted_df[6]
    clean_df1["PlanNonPlan"]= splitted_df[7]
    clean_df1["VotedCharged"]= splitted_df[8]
    clean_df1["StatementofExpenditure"]= splitted_df[9]

    #renaming columns
    clean_df1.rename(columns = {'Sanction Budget\n(April)':'Sanction Budget', 
        'Revised Budget\n(A)':'Revised Budget','Balance\n(A-B)':'Balance',
        'Expenditure\n(within selected period) (B)':'Expenditure'}, inplace = True)

def store_in_sqlite():
    #Connect to the SQLite3 database
    conn = sqlite3.connect('mydb.db')

    #Store the transformed data in the database
    clean_df1.to_sql('hp_table', conn, if_exists='replace', index=False)

    #commit and close the connection
    conn.commit()
    conn.close()

with DAG('my_data_pipeline', default_args=default_args, schedule=timedelta(days=1)) as dag:

    scrape_data_task = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    store_in_sqlite_task = PythonOperator(
        task_id='store_in_sqlite',
        python_callable=store_in_sqlite
    )

    # Define task dependencies
    scrape_data_task >> transform_data_task >> store_in_sqlite_task
