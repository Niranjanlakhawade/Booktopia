import requests
import pandas as pd
import concurrent.futures
import traceback
import csv
import json
from datetime import *
from bs4 import BeautifulSoup
from tqdm import tqdm


def response(url,re_try=0):
    source = requests.get(url)
    if source.status_code !=200:            
	# Retry up to 4 times for a successful 200 response
        re_try=re_try+1
        # print(url,re_try)
        if re_try < 4:
            response(url,re_try)
        else:
	    # Log URLs that failed to respond successfully after retries
            with open('Booktopia_PNF_' + str(dat) + '.csv', 'a', newline='', encoding='UTF-8') as fp:
                write = csv.writer(fp)
                write.writerow([url,source.status_code,'Book Not Found'])
    else:
        soup= BeautifulSoup(source.content,'lxml')
        try:
	    # Extract JSON data embedded in the page
            json_data = json.loads(soup.select_one('script#__NEXT_DATA__').text)
           
	    # Extract necessary fields from the JSON data
            try:
                title= json_data['props']['pageProps']['product']['displayName']
            except:
                title =''
            try:
                auther= [author_['role'] for author_ in json_data['props']['pageProps']['product']['contributors'] if author_['role'] =='Author'][0]
            except:
                auther =''
            try:
                product_url= url
            except:
                product_url =''
            try:
                book_type= json_data['props']['pageProps']['product']['bindingFormat']
            except:
                book_type =''
            try:
                original_price= json_data['props']['pageProps']['product']['retailPrice']
                if original_price == 0:
                    original_price =''
            except:
                original_price =''
            try:
                discounted_price= json_data['props']['pageProps']['product']['salePrice']
                if discounted_price == 0:
                    discounted_price= ''
            except:
                discounted_price =''
            try:
                isbn_10= json_data['props']['pageProps']['product']['isbn10']
            except:
                isbn_10 =''
            try:
                published_date = json_data['props']['pageProps']['product']['publicationDate']
            except:
                published_date = ''
            try:
                publisher = json_data['props']['pageProps']['product']['publisher']
            except:
                publisher = ''
            try:
                no_pages = json_data['props']['pageProps']['product']['numberOfPages']
            except:
                no_pages = ''
	   # Create a dictionary with the extracted data
            temp={'Title':title,'Auther':auther,'Product_URL':product_url,'Book_type':book_type,'Original_Price':original_price,'Discounted_Price':discounted_price,
                  'ISBN_10':isbn_10,'Published_Date':published_date,'Publisher':publisher,'No_of_pages':no_pages}
            
		
	   # Append the data to the output CSV file
            with open('output_booktopia_' + str(dat) + '.csv', 'a', newline='', encoding='UTF-8') as file:
                writer = csv.DictWriter(file,fieldnames=temp.keys())                
                writer.writerow(temp)
	   # Update the progress bar
            pbar.update(1)                          
            return None
        except:
            print(traceback.format_exc())



if __name__ == '__main__':
    
    # Load ISBN numbers from input CSV file
    list_isbi_nos = pd.read_csv('isbi_no.csv', header=0)['input'].to_list()
    dat = date.today()
    
    # Create output CSV file with headers
    with open('output_booktopia_' + str(dat) + '.csv', 'a', newline='', encoding='UTF-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title','Auther','Product_URL','Book_type','Original_Price','Discounted_Price',
                  'ISBN_10','Published_Date','Publisher','No_of_pages'])

    # Initialize progress bar
    pbar = tqdm(total = len(list_isbi_nos), desc='Working: ')
    try:
	# Use ThreadPoolExecutor for concurrent URL requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_url = {executor.submit(response,'https://www.booktopia.com.au/book/'+str(isbi_no)+'.html') for isbi_no in list_isbi_nos}

    except Exception as e:
        traceback.format_exc()

