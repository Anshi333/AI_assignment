import advertools as adv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime
import pytz
import os
import uuid  
from flask import send_from_directory
from urllib.parse import urlparse
import ua_parser
from ua_parser import user_agent_parser
import pyarrow.parquet as pq
import pyarrow
from io import BytesIO
import base64
from ipywidgets import interact
pd.options.display.max_columns = None

for p in [adv, pd, pyarrow]:
    print(f'{p.__name__:-<14}v{p.__version__}')

# Load logs to DataFrame

adv.logs_to_df(
    log_file='log_file.log',
    output_file='output_file.parquet',
    errors_file='errors_file.txt',
    log_format='combined')

# Extract basic information
logs_df = pd.read_parquet('output_file.parquet')
logs_df['datetime'] = pd.to_datetime(logs_df['datetime'],
                                     format='%d/%b/%Y:%H:%M:%S %z',utc=True)

# Extract user agent information
ua_df = pd.json_normalize([user_agent_parser.Parse(ua) for ua in logs_df['user_agent']])
ua_df.columns = 'ua_' + ua_df.columns.str.replace('user_agent\.', '', regex=True)

def save_plot_as_image(plt):
    # Save plot as bytes in memory
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png')
    plt.close()
    img_buffer.seek(0)
    
    # Encode the plot image as base64
    img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
    
    return img_base64

def get_logfile_head(logs_df):
        return logs_df.head()

def get_logfile_tail(logs_df):
        return logs_df.tail()
    
def get_total_records(logs_df):
        return logs_df.shape[0]

def get_total_columns(logs_df):
        return logs_df.shape[1]

def get_unique_ip_addresses(logs_df):
        data = logs_df["client"].unique()
        total_ipaddresses = len(data)
        return total_ipaddresses, data

def get_request_count_per_ip(logs_df):
        data1 = logs_df["client"].value_counts().reset_index()
        data1.columns = ['ip_address', 'request_count']
        data1.sort_values(by='request_count', ascending=False)
        return data1

def get_top10_hits_per_ip_address(logs_df):
        data1 = logs_df["client"].value_counts().reset_index()
        data1.columns = ['ip_address', 'request_count']
        dt1=data1.head(10)
        x=dt1["ip_address"]
        y=dt1['request_count']
        plt.plot(x,y,color="gold",linewidth=1,marker="o",markersize=2)
        plt.xticks(rotation=75,fontsize=7)
        plt.xlabel("IP ADDRESS", color="blue")
        plt.ylabel("REQUEST COUNT" , color="red")
        plt.title("IP ADDRESS vs REQUEST COUNT for TOP 10 IP ADDRESSES" , color="indigo")
        plt.grid(axis = 'y')
        m = save_plot_as_image(plt)
        return dt1,m

def get_request_count_per_httpcode(logs_df):
        data5 = logs_df["referer"].value_counts().reset_index()
        data5.columns = ['http codes', 'request_count']
        data5.sort_values(by='request_count', ascending=False)
        return data5

def get_top10_hits_per_http_codes(logs_df):
        data5 = logs_df["referer"].value_counts().reset_index()
        data5.columns = ['http codes', 'request_count']
        dt2=data5.head(10)
        x=dt2["http codes"]
        y=dt2['request_count']
        plt.plot(x,y,color="darkgoldenrod",linewidth=1,marker="o",markersize=2)
        plt.xticks(rotation=80,fontsize=7)
        plt.xlabel("HTTP CODES", color="green")
        plt.ylabel("REQUEST COUNT" , color="purple")
        plt.title("HTTP CODES vs REQUEST COUNT for TOP 10 HTTP CODES" , color="teal")
        plt.grid(axis = 'y')
        n=save_plot_as_image(plt)
        return dt2,n

def get_unique_http_codes(logs_df):
        data2 = logs_df["referer"].unique()
        total_httpcode=len(data2)
        return total_httpcode, data2

def get_request_count_per_URL(logs_df):
        data6=logs_df["request"].value_counts().reset_index()
        data6.columns = ['URL', 'hits_count']
        data6.sort_values(by='hits_count', ascending=False)
        return data6

def get_URL_max_hits(logs_df):
        data6=logs_df["request"].value_counts().reset_index()
        data6.columns = ['URL', 'hits_count']
        data6.sort_values(by='hits_count', ascending=False)
        return data6.head(1)

def get_top10_hits_per_URL(logs_df):
        data6=logs_df["request"].value_counts().reset_index()
        data6.columns = ['URL', 'hits_count']
        dt3=data6.head(10)
        x=dt3["URL"]
        y=dt3['hits_count']
        plt.plot(x,y,color="rebeccapurple",linewidth=1,marker="o",markersize=2)
        plt.xticks(rotation=80,fontsize=7)
        plt.xlabel("URL", color="darkorange")
        plt.ylabel("HITS COUNT" , color="deeppink")
        plt.title("URL vs HITS COUNT for TOP 10 URL" , color="royalblue")
        plt.grid(axis = 'y')
        k=save_plot_as_image(plt)
        return dt3,k

def get_request_count_per_Platform(ua_df):
        data7=ua_df["ua_os.family"].value_counts().reset_index()
        data7.columns = ['Platform', 'hits_count']
        data7.sort_values(by='hits_count', ascending=False)
        return data7

def get_top10_hits_per_Platform(ua_df):
        data7=ua_df["ua_os.family"].value_counts().reset_index()
        data7.columns = ['Platform', 'hits_count']
        dt4=data7.copy()
        x=dt4["Platform"]
        y=dt4['hits_count']
        plt.plot(x,y,color="maroon",linewidth=1,marker="o",markersize=2)
        plt.xticks(rotation=35,fontsize=8)
        plt.xlabel("PLATFORM", color="crimson")
        plt.ylabel("HITS COUNT" , color="midnightblue")
        plt.title("PLATFORM vs HITS COUNT" , color="darkslategray")
        plt.grid(axis = 'y')
        l=save_plot_as_image(plt)
        return dt4,l

def get_request_count_per_Browser(ua_df):
        data8=ua_df["ua_family"].value_counts().reset_index()
        data8.columns = ['Browser', 'hits_count']
        data8.sort_values(by='hits_count', ascending=False)
        return data8

def get_top10_hits_per_Browser(ua_df):
        data8=ua_df["ua_family"].value_counts().reset_index()
        data8.columns = ['Browser', 'hits_count']
        dt5=data8.head(10)
        x=dt5["Browser"]
        y=dt5['hits_count']
        plt.plot(x,y,color="mediumvioletred",linewidth=1,marker="o",markersize=2)
        plt.xticks(rotation=75,fontsize=8)
        plt.xlabel("BROWSER", color="darkgreen")
        plt.ylabel("HITS COUNT" , color="firebrick")
        plt.title("BROWSER vs HITS COUNT for TOP 10 BROWSERS" , color="steelblue")
        plt.grid(axis = 'y')
        o=save_plot_as_image(plt)
        return dt5,o

def get_hits_count_per_hr(logs_df):
        logs_df['datetime'] = logs_df['datetime'].dt.tz_convert('UTC')
        logs_df['hour'] = logs_df['datetime'].dt.hour
        hits_per_hr = logs_df['hour'].value_counts().reset_index()
        hits_per_hr.columns = ['Hits per hr', 'hits_count']
        hits_per_hr.sort_values(by='hits_count', ascending=False)
        return hits_per_hr

def get_top10_hits_per_hr(logs_df):
        logs_df['datetime'] = logs_df['datetime'].dt.tz_convert('UTC')
        logs_df['hour'] = logs_df['datetime'].dt.hour
        hits_per_hr = logs_df['hour'].value_counts().reset_index()
        hits_per_hr.columns = ['Hits per hr', 'hits_count']
        dt6=hits_per_hr.head(10)
        x=dt6["Hits per hr"]
        y=dt6['hits_count']
        plt.plot(x,y,color="darkolivegreen",linewidth=1)
        plt.xlabel("HITS PER HR", color="orangered")
        plt.ylabel("HITS COUNT" , color="dodgerblue")
        plt.title("HITS PER HR vs HITS COUNT for TOP 10 HITS PER HR" , color="navy")
        plt.grid(axis = 'y')
        r=save_plot_as_image(plt)
        return dt6,r

def get_traffic_distribution_hrly(logs_df):
        data8=logs_df.copy()
        data8['hour'] = data8['datetime'].dt.hour
        site_hourly_traffic = data8.groupby(['referer', 'hour'])['size'].sum().reset_index()
        total_traffic_per_hour = data8.groupby('hour')['size'].sum().reset_index()
        traffic_distribution = pd.merge(site_hourly_traffic, total_traffic_per_hour, 
                                        on='hour', suffixes=('_site', '_total'))
        traffic_distribution['distribution'] = traffic_distribution['size_site'] / traffic_distribution['size_total']
        return traffic_distribution
    
def Automation_log_analysis(logs_df):
        def display_menu():
            print("Menu:")
            print("1. Head of log access file ")
            print("2. Tail of log access file")
            print("3. Total records")
            print("4. Total columns")
            print("5. Top 10 hits per ip address")
            print("6. Total hits per http code")
            print("7. Total hits per http code ")
            print("8. Top 10 hits per http code")
            print("9. Unique http codes and there total coun")
            print("10. Head of log access file ")
            print("11. Total hits per Url")
            print("12. Url with maximum hits")
            print("13. Top 10 hits per URL ")
            print("14. Total hits per Platform")
            print("15. Top 10 hits per PLatform")
            print("16. Total hits per Browser ")
            print("17. Top 10 hits per Borwser")
            print("18. Total hits per hour")
            print("19. Top 10 hits hourly basis ")
            print("20. Hourly traffic distribution")
            print("21. Exit")
                  
        def option1():
            return get_logfile_head(logs_df)

        def option2():
            return get_logfile_tail(logs_df)
         
        def option3():
            return get_total_records(logs_df)
                  
        def option4():
            return get_total_columns(logs_df)
                  
        def option5():
            return get_unique_ip_addresses(logs_df)
                  
        def option6():
            return get_request_count_per_ip(logs_df)
                  
        def option7():
            return get_top10_hits_per_ip_address(logs_df)
                  
        def option8():
            return get_request_count_per_httpcode(logs_df)
                  
        def option9():
            return get_top10_hits_per_http_codes(logs_df)
                  
        def option10():
            return get_unique_http_codes(logs_df)
                  
        def option11():
            return get_request_count_per_URL(logs_df)
                  
        def option12():
            return get_URL_max_hits(logs_df)
                  
        def option13():
            return get_top10_hits_per_URL(logs_df)
                  
        def option14():
            return get_request_count_per_Platform(ua_df)
                  
        def option15():
            return get_top10_hits_per_Platform(ua_df)
                  
        def option16():
            return get_request_count_per_Browser(ua_df)
                  
        def option17():
            return get_top10_hits_per_Browser(ua_df)
                  
        def option18():
            return get_hits_count_per_hr(logs_df)
                  
        def option19():
            return get_top10_hits_per_hr(logs_df)
                  
        def option20():
            return get_traffic_distribution_hrly(logs_df)
                  

        def main():
            while True:
                display_menu()
                choice = input("Enter your choice: ")

                if choice == '1':
                    return option1()
                elif choice == '2':
                    return option2()
                elif choice == '3':
                    return option3()
                elif choice == '4':
                    return option4()
                elif choice == '5':
                    return option5()
                elif choice == '6':
                    return option6()
                elif choice == '7':
                    return option7()
                elif choice == '8':
                    return option8()
                elif choice == '9':
                    return option9()
                elif choice == '10':
                    return option10()
                elif choice == '11':
                    return option11()
                elif choice == '12':
                    return option12()
                elif choice == '13':
                    return option13()
                elif choice == '14':
                    return option14()
                elif choice == '15':
                    return option15()
                elif choice == '16':
                    return option16()
                elif choice == '17':
                    return option17()
                elif choice == '18':
                    return option18()  
                elif choice == '19':
                    return option19()
                elif choice == '20':
                    return option20()
                elif choice == '21':
                    print("Exiting the program.")
                    break
                else:
                    print("Invalid choice. Please select a valid option.")
def report_log_analysis(logs_df,ua_df):
        print("Head of log access file contains -\n",get_logfile_head(logs_df))
        print("Total no. of rows/records in log access file is -\n",get_total_records(logs_df))
        print("Total no. of columns in log access file is -\n",get_total_columns(logs_df))
        print("Total hits per ip address -\n",get_request_count_per_ip(logs_df))
        print("Top 10 hits per ip address -\n",get_top10_hits_per_ip_address(logs_df))
        print("Total hits per http code -\n",get_request_count_per_httpcode(logs_df))
        print("Top 10 hits per http code -\n",get_top10_hits_per_http_codes(logs_df))
        print("Unique http codes and there total count -\n",get_unique_http_codes(logs_df))
        print("Total hits per Url -\n",get_request_count_per_URL(logs_df))
        print("Url with maximum hits -\n",get_URL_max_hits(logs_df))
        print("Top 10 hits per URL -\n",get_top10_hits_per_URL(logs_df))
        print("Total hits per Platform -\n",get_request_count_per_Platform(ua_df))
        print("Top 10 hits per PLatform\n",get_top10_hits_per_Platform(ua_df))
        print("Total hits per Browser -\n",get_request_count_per_Browser(ua_df))
        print("Top 10 hits per Borwser -\n",get_top10_hits_per_Browser(ua_df))
        print("Total hits per hour -\n",get_hits_count_per_hr(logs_df))
        print("Top 10 hits hourly basis -\n",get_top10_hits_per_hr(logs_df))
        print("Hourly traffic distribution -\n",get_traffic_distribution_hrly(logs_df))
        return