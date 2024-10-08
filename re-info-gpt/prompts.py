import json

tech_sectors_path = 'input_data/tech_sectors.json'
business_types_path = 'input_data/business_types.json'
customer_classes_path = 'input_data/customer_classes.json'
products_path = 'input_data/products.json'

with open(tech_sectors_path, 'r') as tech_sectors_file, open(business_types_path, 'r') as business_file, open(customer_classes_path, 'r') as customer_file, open(products_path, 'r') as products_file:
    products_list = json.load (products_file)
    tech_sectors = json.load(tech_sectors_file)
    business_types = json.load (business_file)
    customer_classes = json.load (customer_file)

def info_prompt(site_text):
    return f"""
        consider the following information from the company's website:
        {site_text} 

        using the information you have just read, while providing reasoning and references for your answers, conclude the following :
        
        1.Business description (max 150 words)
         
        2.Indicate which of the following business types best describe what the company does:
          {business_types}
          - Primary Business Type
          - Secondary Business Type (Write 'Other' if exact match not found)
        
        3.You MUST Determine the products offered by the business if its primary or secondary business types contains :
          -Manufacturer
          -Distributor/Reseller
          -Software
          If EXACT product names cannot be found, list all general products they offer instead.

        4.Which of the following customer class do they serve?
          {customer_classes} 

        5.Identify the technology sectors (Choose more than one, if applicable) from the products/services using {tech_sectors}?
        
        6. Do not attempt to identify products for service-oriented companies.
        
        7. If you cannot extract relevant information from any field in the provided data, respond with 'Not Found'.
        
        8. Identify the primary business type accurately. 
        
        9. If a company provide any products/services other than primary business type, put it in the secondary business type(s)

        10. Exclude the websites with improper or no data.
 
        11. Also make sure:            
          - You MUST NEVER return products that are not present in the site data.
          - You MUST NEVER return products from service-based or development companies
          - You MUST always provide reasoning for your response in the reasoning section.
          - You MUST always identify the technology sector by looking at the products and services
          - You MUST classify all products/services in a technology sector from the given sectors only
          - You MUST consider the complete company info before idenitfying the business type(s)
          - YOU MUST return your answers in the following JSON format:


        {{"reasoning_and_references":"reasoning","normal_business_description":"concise business description concluded as a paragraph","primary_business_type":"main business type only","secondary_business_type":["type 1", "type 2"] (comma separated list) ,"products":["Product 1", "Product 2", "Product 3"]  (comma seperated list),"customer_class":["class 1", "class 2"](comma seperated list),"technology_sector":["sector 1", "sector 2"] (comma seperated list)"}}
      
        
        """


def info_prompt_without_services(site_text):
    return f"""
        consider the following information from the company's website:
        {site_text}.

        using the information you have just read, while providing reasoning and references for your answers,conclude the following :
        
        1.Business description 
        
        2.Which of the following business types would fit it best: 
        {business_types}

        3.What products do they offer, if any?

        4.Which of the products from the following list do they offer:
        
        5.Which of the following customer class do they serve?
          Residential
          Community Energy
          Commercial
          Industrial
          Utility
          Government
          Agriculture
          Military and Defense
          Healthcare
          Public Transportation

        6.Which of the following technology sectors do they service?
          Solar
          Energy Storage
          Hydrogen
          Wind
          E-Mobility (electric vehicles)
          Microgrids
          Geothermal
          Carbon Capture
          Marine & Hydro
          Bioenergy
          Smart Grid
          Energy Efficiency	
        
        - You MUST NEVER return products that are not present in the site data.
        - You MUST NEVER return information that is not explicitly mentioned in the site data.
        - You MUST always provide resoning for your response in the reasoning section.
        - YOU MUST return your answers in the following JSON format:

        {{"reasoning_and_references":"reasoning and references","normal_business_description":"concise business description concluded as a paragraph”,"primary_business_type":"main business type only","secondary_business_type":"other business types concluded (semi colon-separated)","products":"list of products offered (comma-separated)","customer_class":"customer class concluded","technology_sector":"technology sector concluded (comma-separated)"}}
        
        
        """

def prod_match_prompt(given_products):
    return f"""
        consider the following products:

        {given_products}

        Match the given products to the closest one from this PRODUCT LIST: {products_file}
        
        - Match the Given Products To the Products List (Closest Matches, if not Exact)
        - Do Not match any products if the given products are empty
        - return "NO MATCH" where a product doesnt match anything from the list
        - If a product has more than one names (put the other names in a parenthesis)
        - You Must return your answer in the following JSON fromat:

        {{"product": "matching results from the list","product": "matching results from the list"}}

        ######example#####
        {{"Other Charging Systems": "PV-ESS-Charging solutions","Intelligent control systems": "Control Systems (Yaw, Pitch, Brake, etc.)","Energy Efficiency":"No Match"}}

        """


def johns_prompt(site_text):
    return f"""
        Consider the following information from the company's website:
        {site_text} 

        Using the information you have just read, do the following:
        
        Step 1. Provide a business description for the company in 150 words or less.

        Step 2. Indicate which of the following business types best describe what the company does:    
          {business_types}
          - Primary Business Type (Write 'Other' if exact match not found)
          - Secondary Business Type (Write 'Other' if exact match not found)
 
        Step 3. If you determined that the company is a "Distributor/Reseller", "Manufacturer", or "Software Provider", identify what products they offer. If you can't identify any products, return "Not Found".
        
        Step 4. Identify which of the following technology sectors the company is involved in:
        {tech_sectors}

        Step 5. Here is a dictionary of products and their corresponding technology sector(s). Filter these products so they only include those that are tied to the sectors that you identified in step 4.
        {products_list}

        here's what this should look like:
        {{{{"Commercial Electric Vehicles": ["E-Mobility","Hydrogen"],
        "Energy-Efficient Lighting":["Energy Efficiency"],
        "Construction Equiptment":["Solar","Wind"]}}}}

        Step 6. You have to map all products that you identified in step 3 to one of the products in the filtered list that you just created in step 5. Make Sure That No Product is missed. If you can't find a matching product or are uncertain, return "Not Found".

        Step 7. Indicate which of following types of customers the company serves.
        {customer_classes}

        Return your responses in the following JSON format:

        {{{{"business_description": output from step 1 formatted as paragraph enclosed in quotes,
        "primary_business_types": output from step 2 formatted as comma separated list (e.g., ["business type 1","business type 2", "Other"]),
        "secondary_business_types": output from step 2 formatted as comma separated list (e.g., ["business type 1","business type 2", "Other"]),
        "products":"output from step 3 formatted as comma seperated list (e.g., ["product 1","product 2"]),
        "technology_sectors": output from step 4 formatted as a comma seperated list (e.g., ["sector 1","sector 2"]),
        "mapped_products": output from step 6 formatted as a dictonary with key value pairs (e.g., {{"original product 1":"matched product 1","original product 2":"matched product 2"}})
        "customer_types": output from step 7 formatted as a comma seperated list (e.g., ["customer type 1","customer type 2"])}} }}
        
        
        """