import requests
import json

dataset_descriptions = []

#obtain the oids of the objects contained in the nodes
def get_unique_ids(array): 
    unique_ids = []
    unique_id_strings = []
    for item in array:
        # Obtener el ID después de "graph:"
        _, id_value = item.split('https://oeg.fi.upm.es/wothive/')

        # Verificar si el ID ya está en la lista unique_ids 
        if id_value not in unique_ids:
            unique_ids.append(id_value)
            unique_id_strings.append("https://oeg.fi.upm.es/wothive/" + id_value)
    return unique_ids, unique_id_strings



#filter the triples by the oid of the object and obtain the metadata modeled in the tiples
def obtain_metadata(response):
    # Step 1: Obtain a list of all "sub" values
    subs = [binding['sub']['value'] for binding in response['message']['results']['bindings'] if binding['sub']['value'].startswith('https://oeg.fi.upm.es/wothive/')]
    oids, oids_sub=get_unique_ids(subs) 
    #print(oids, oids_sub)
    #print(oids_sub)
    print("Number of Datasets: "+str(len(oids_sub)))
    
    #Create a list of datasets
    thing_descriptions = []
    
    # Step 2-4: Iterate over the "sub" values and create the thing descriptions
    for sub in oids_sub:  # Using set to obtain unique values
        # Step 2: Filter the triples for the current object
        print("OID Dataset: "+str(sub))
        filtered_results = [binding for binding in response['message']['results']['bindings'] if sub.__contains__(binding['sub']['value'])]
        
        # Define the dictionary for the dataset information
        dataset_data = {
            'name': '',
            'title': '',
            'domain': '',
            'owner_org': '',
            'description': '',
            'resources': [
                {
                    'name': '',
                    'url': '',
                }
            ]
        }
        
        # Iterate over the triples and obtain the metadata values
        for triple in filtered_results:
            subject = triple['sub']['value']
            predicate = triple['p']['value']
            object_type = triple['o']['type']
            object_value = triple['o']['value']

            # Update dataset_data based on the triple and the information from the WoT TD
            if predicate == 'https://www.w3.org/2019/wot/td#title': #NAME
                dataset_data['name'] = object_value
            elif predicate == 'https://www.w3.org/2019/wot/td#serviceName': # RESOURCE - TITLE
                dataset_data['title'] = object_value
                dataset_data['resources'][0]['name'] = object_value
            elif predicate == 'https://www.w3.org/2019/wot/td#hasURL': #RESOURCE - URL
                dataset_data['resources'][0]['url'] = object_value
            elif predicate == 'https://www.w3.org/2019/wot/td#provider': #OWNER ORG
                dataset_data['owner_org'] = object_value
            if predicate == 'https://www.w3.org/2019/wot/td#serviceDescription': #DESCRIPTION
                dataset_data['description'] = object_value
            elif predicate == 'https://www.w3.org/2019/wot/td#hasDomain':
                dataset_data['domain'] = object_value
        print(json.dumps(dataset_data, indent=4))
        #append datasets to a single dictionary   
        dataset_descriptions.append(dataset_data) 
        
    # Step 5: Return the list of datasets
    return dataset_descriptions

# Obtain Community ID
urlcommId = "http://10.45.0.16:81/api/collaboration/communities"
payload = {}
headers = {
    'Accept': 'application/json',
    'Content-Type': 'text/plain'
}
response = requests.request("GET", urlcommId, headers=headers, data=payload)
commId = response.json()
commId = commId['message'][0]['commId']
print(f"Community ID: {commId}")



#sparql query to obtain the triples of the thing descriptions
query = '''
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
      PREFIX wot: <https://www.w3.org/2019/wot/td#> 
      SELECT distinct ?p ?o ?sub WHERE { 
      ?sub rdf:type wot:Service . 
      ?sub ?p ?o .}
'''


#change to put the url of the remote query
url = f"http://10.45.0.16:81/api/discovery/remote/semantic/community/{commId}"

try: 
    # Send SPARQL query request and process the information to create a list of datasets and its metadata
    response = requests.post(url, headers=headers, data=query, timeout=100000) # Timeout 30000 seg
    response.raise_for_status()

    # Process the response correctly if there were no errors 
    data = response.json()

    # Obtain the metadata of all the datasets 
    datasets=obtain_metadata(data)
    #print(json.dumps(datasets, indent=4))
except requests.exceptions.Timeout as e:
    print("Waiting time exhausted when making the query")
except requests.exceptions.RequestException as e:
    print(f"Query error: {e}")
