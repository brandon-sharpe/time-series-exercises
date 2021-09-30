def get_items():
    '''
    returns dataframe of all items either through system cache or via an api
    '''
    if os.path.isfile('items.csv'):
        df = pd.read_csv('items.csv')
        return df
    else: 
        items_list = []
    
        response = requests.get(base_url+'/api/v1/items')
        data = response.json()
        n = data['payload']['max_page']
    
        for i in range(1,n+1):
            url = base_url+'/api/v1/items?page='+str(i)
            response = requests.get(url)
            data = response.json()
            page_items = data['payload']['items']
            items_list += page_items
        
        df = pd.DataFrame(items_list)
            
            
        df.to_csv('items.csv', index=False)
    return df
    

def get_stores():
    '''returns dataframe of all items either through system cache or via an api'''
    if os.path.isfile('stores.csv'):
        df = pd.read_csv('stores.csv')
        return df
    else: 
        # Creates a list to be used later
        stores_list = []
        # url for sales data
        url = base_url+'/api/v1/stores'
        # Request/response
        response = requests.get(url)
        # python readble
        data = response.json()
        # number of iterations
        n = data['payload']['max_page']
        
        # make the itterations
        for i in range(1,n+1):
            #this iterates through the pages
            stores_url = url + '?page=' +str(i)
            response = requests.get(stores_url)
            data = response.json()
            page_stores = data['payload']['stores']
            stores_list += page_stores
        
        df = pd.DataFrame(stores_list)
            
            
        df.to_csv('stores.csv', index=False)
    return df 

def get_sales():
    '''Either reads a sales.csv stored on system  or creates
    a dataframe cointaing all sales data and returns a csv'''
    
    if os.path.isfile('sales.csv'):
        df = pd.read_csv('sales.csv')
        return df
    
    else: 
        # Creates a list to be used later
        sales_list = []
        # url for sales data
        url = base_url+'/api/v1/sales'
        # Request/response
        response = requests.get(url)
        # python readble
        data = response.json()
        # number of iterations
        n = data['payload']['max_page']
    
        # make the itterations
        for i in range(1,n+1):
            #this iterates through the pages
            sales_url = url + '?page=' +str(i)
            response = requests.get(sales_url)
            data = response.json()
            page_sales = data['payload']['sales']
            sales_list += page_sales
        
        df = pd.DataFrame(sales_list)
            
            
        df.to_csv('sales.csv', index=False)
    return df 
            

def composite_sales_data(sales_df, items_df, stores_df):
    '''Merges sales, item, and store dataframes. You must 
    arange arguments in the order of sales, items, and stores dataframes.
    returns a composite dataframe'''
    sales_df = sales_df.rename(columns = {'item':'item_id'})
    df = sales_df.merge(items_df, on = 'item_id', how = 'left')
    df = df.rename(columns = {'store':'store_id'})
    df = df.merge(stores_df ,on = 'store_id', how = 'left')
    return df

def Gfuel():
    '''Returns german power data'''
    return pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')