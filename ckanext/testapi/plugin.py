import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import os
import ckanapi
from apscheduler.schedulers.background import BackgroundScheduler
import random
import string
from datetime import datetime

# Replace with your CKAN instance URL

# Define the data for the new dataset
organization_name = 'deustoprueba'
organization_data = {
    'name': organization_name,
    'title': 'University of Deusto - prueba',
    'description': 'Your organization description'
}

ckan_url=os.getenv('CKAN_SITE_URL')
ckan_key=os.getenv('CKAN_API_KEY')

def create_datasets():
    length = 2
    name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    title =str(datetime.now())
    #title =''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    dataset_data = {
    'name': name,
    'title': title,
    'notes': 'This is an example dataset2',
    'owner_org': organization_name,
    'resources': [
        {
            'name': 'example-resource',
            'url': 'https://zenodo.org/record/4681206',
        }
    ]
    }
    
    # Read metadata from a JSON file
    print("Executing Task...")
    ckan = ckanapi.RemoteCKAN(ckan_url, apikey= ckan_key)
    # Check if the organization exists
    try:
        ckan.action.organization_show(id=organization_name)
        print(ckan.action.organization_show(id=organization_name))
    except ckanapi.NotFound:
        # If the organization doesn't exist, create it
        ckan.action.organization_create(**organization_data)
        print('Organization created successfully!')

    # Create a new dataset on CKAN
    try:
        ckan.action.package_create(**dataset_data)
        print('Dataset created successfully!')
    except ckanapi.errors.ValidationError:
        print('Dataset already exists')

  
        
class TestapiPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)

    # IConfigurer
    def update_config(self, config_):
        print("update config")
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic','testapi')    
        #create_datasets()
    
        self.scheduler = BackgroundScheduler()   
   
        self.scheduler.add_job(
            create_datasets,  # The function to run
            'interval',  # Run the job at a specified interval
            seconds=60 # Run the job every hour
        )
        self.scheduler.start()
 


